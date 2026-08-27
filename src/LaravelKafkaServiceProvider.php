<?php

declare(strict_types=1);

namespace LaravelKafka;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Queue\Factory as QueueFactoryContract;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\Worker;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\ServiceProvider;
use LaravelKafka\Console\DlqTailCommand;
use LaravelKafka\Console\HorizonSnapshotCommand;
use LaravelKafka\Console\ReplayCommand;
use LaravelKafka\Console\WorkCommand;
use LaravelKafka\Delay\DelayRouter;
use LaravelKafka\Manager\ConnectionFactory;
use LaravelKafka\Manager\KafkaManager;
use LaravelKafka\Queue\Failed\DatabaseFailedJobHandler;
use LaravelKafka\Queue\Failed\FailedJobHandlerFactory;
use LaravelKafka\Queue\KafkaConnector;

/**
 * laravel-kafka 主 ServiceProvider（v0.1）。
 *
 * ## 启动顺序
 *
 *  1. `register()`：merge 配置 + 单例绑定（ConnectionFactory / KafkaManager / `kafka.manager` alias）
 *  2. `boot()`：
 *     - `syncFailedTableConfig()` 同步 failed table 到 `queue.failed`
 *     - `registerFailer()` 绑定 `queue.failer.kafka`（仅 database/hybrid 模式）
 *     - `registerFailedHandlerEvent()` 监听 `JobFailed` 事件
 *     - `registerCommands()` 注册 `kafka:work` 命令
 *     - `registerPublishing()` 暴露 `php artisan vendor:publish --tag=kafka-config`
 *
 * ## 业务方使用
 *
 * ```php
 * // config/app.php
 * 'providers' => [
 *     LaravelKafka\LaravelKafkaServiceProvider::class,
 * ],
 * ```
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 启动时**没有** `syncFailedTableConfig`，
 * 业务方必须自己改 `queue.failed`；我们自动同步 → 业务方零配置。
 */
final class LaravelKafkaServiceProvider extends ServiceProvider
{
    /**
     * @var bool
     */
    protected $defer = false;

    /**
     * 注册容器绑定。
     *
     * - `ConnectionFactory` 单例：装配 Producer / Consumer / Failed 三 Factory
     * - `KafkaManager` 单例：从 `config('kafka.connections')` 注册所有 connection
     * - `kafka.manager` alias：内部 `WorkCommand` 等用
     * - `Queue::extend('kafka', ...)`：让 `config/queue.php` 配 `driver => 'kafka'` 时识别
     *
     * @return void
     */
    public function register(): void
    {
        $this->mergeConfigFrom(
            __DIR__ . '/../config/kafka.php',
            'kafka'
        );

        $this->app->singleton(ConnectionFactory::class, function ($app) {
            return new ConnectionFactory(
                $app->make(\LaravelKafka\Producer\ProducerFactory::class),
                $app->make(\LaravelKafka\Consumer\ConsumerFactory::class),
                $app->make(FailedJobHandlerFactory::class),
                $app,
            );
        });

        $this->app->singleton(KafkaManager::class, function ($app) {
            $manager = new KafkaManager($app->make(ConnectionFactory::class));
            $connections = (array) config('kafka.connections', []);
            $manager->registerConnections($connections);
            return $manager;
        });

        // 容器别名，ServiceProvider 内部使用
        $this->app->alias(KafkaManager::class, 'kafka.manager');

        // v0.3 Step 2: 时间轮分层路由（从 kafka.connections.default.delay 读配置）
        $this->app->singleton(DelayRouter::class, function ($app) {
            $delayConfig = (array) config('kafka.connections.default.delay', []);
            $tiers = (array) ($delayConfig['tiers'] ?? [5, 30, 60, 300, 1800, 3600, 86400]);
            $prefix = (string) ($delayConfig['topic_prefix'] ?? 'delay');
            return new DelayRouter($tiers, $prefix);
        });

        // queue.connector.kafka：移到 boot() 阶段（v0.1 老 bug 修复）
        // register() 阶段 Queue Facade 还没解析（QueueServiceProvider 还没 register 完）
    }

    /**
     * Boot 阶段（所有 ServiceProvider register 完成后）。
     *
     * @return void
     */
    public function boot(): void
    {
        // v0.4.1 hotfix: 给 Worker::class 加 alias 指向 'queue.worker' 单例
        // (register() 阶段 QueueServiceProvider 还没跑, 'queue.worker' 未绑, 所以必须放 boot())
        if ($this->app->bound('queue.worker') && ! $this->app->isShared(Worker::class)) {
            $this->app->alias('queue.worker', Worker::class);
        }

        // v0.4.1 hotfix: FailedJobHandlerInterface 是接口, 容器无法自动解析.
        // NativeHandler 构造声明 FailedJobHandlerInterface 时会触发 "is not instantiable".
        // 绑到 FailedJobHandlerFactory::makeFor(default_config) 让容器按 default config 拿具体 handler.
        $this->app->bind(\LaravelKafka\Queue\Failed\FailedJobHandlerInterface::class, function ($app) {
            $config = $app['kafka.manager']->config();
            $factory = $app->make(\LaravelKafka\Queue\Failed\FailedJobHandlerFactory::class);
            return $factory->makeFor($config);
        });

        // v0.4.1 hotfix: Serializer 接口同理, 默认绑 PhpSerializer (与 config('kafka.connections.*.queue')
        // push 用的 PHP serialize 字符串配套; v0.4.1 还不支持 JsonSerializer 作为 default, 见 docs/11-Serializer.md)
        $this->app->bind(\LaravelKafka\Producer\Serializer\Serializer::class, function () {
            return new \LaravelKafka\Producer\Serializer\PhpSerializer();
        });

        // v0.4.1 hotfix: Consumer 类构造声明 RdKafka\KafkaConsumer (容器无法自动解析)
        // 绑到 ConsumerFactory::make() 让 NativeHandler::createJob() make(Consumer::class) 时拿到单例.
        $this->app->bind(\LaravelKafka\Consumer\Consumer::class, function ($app) {
            $config = $app['kafka.manager']->config();
            return $app->make(\LaravelKafka\Consumer\ConsumerFactory::class)->make($config);
        });

        // v0.1 老 bug 修复：Queue::extend 必须放在 boot() 阶段
        // register() 时 QueueServiceProvider 还没 register 完，'queue' 容器绑定不存在
        // 移到 boot()：所有 ServiceProvider 都 register 完了，'queue' 已就绪
        //
        // v0.4.1 hotfix: Laravel 8.x 的 QueueManager::getConnector() 用 call_user_func($this->connectors[$driver])
        // 0 参数调用闭包; Laravel 11+ 才传 1 个 $app 参数. 用 app() helper 兼容两个版本.
        Queue::extend('kafka', function () {
            return new KafkaConnector(app('kafka.manager'));
        });

        $this->syncFailedTableConfig();
        $this->registerFailer();
        $this->registerFailedHandlerEvent();
        $this->registerCommands();
        $this->registerPublishing();
    }

    /**
     * 同步 `kafka.connections.default.failed.database.table` 到 `queue.failed`。
     *
     * 这样 Laravel 内置命令（`queue:failed` / `queue:retry` / `queue:flush`）能正确找到表。
     * dlq 模式不写 failed_jobs 表，跳过同步。
     *
     * @return void
     */
    private function syncFailedTableConfig(): void
    {
        $driver = (string) config('kafka.connections.default.failed.driver', 'hybrid');
        if ($driver === 'dlq') {
            return;
        }
        $table = (string) config('kafka.connections.default.failed.database.table', 'failed_jobs');
        config(['queue.failed' => array_merge(
            (array) config('queue.failed', []),
            ['driver' => 'database-uuids', 'database' => config('kafka.connections.default.failed.database.connection'), 'table' => $table]
        )]);
    }

    /**
     * 注册 `queue.failer.kafka`（Laravel 框架自动注入失败处理器）。
     *
     * 仅 database / hybrid 模式需要（dlq 模式**不**写 failed_jobs 表）。
     *
     * @return void
     */
    private function registerFailer(): void
    {
        $driver = (string) config('kafka.connections.default.failed.driver', 'hybrid');
        if ($driver === 'dlq') {
            // dlq 模式不写 failed_jobs 表，无需注册 failer
            return;
        }

        $this->app->singleton('queue.failer.kafka', function ($app) {
            $config = (array) config('kafka.connections.default.failed.database', []);
            $table = (string) ($config['table'] ?? 'failed_jobs');
            $connection = $config['connection'] ?? null;

            $database = $app->make('db')->connection($connection);

            return new DatabaseFailedJobHandler(
                $database,
                $table,
                $app->make(\Ramsey\Uuid\UuidInterface::class)
            );
        });
    }

    /**
     * 监听 Laravel `JobFailed` 事件。
     *
     * v0.1 占位：{@see KafkaJob::fail()} 已经自己处理了 failed handler 的分发。
     * v0.2 完善：在此处 dispatch {@see \LaravelKafka\Events\MessageFailed} 事件。
     *
     * ## 异常处理
     *
     * 监听器内 try/catch + `ExceptionHandler::report()`，避免监听器异常影响 Laravel 框架。
     *
     * @return void
     */
    private function registerFailedHandlerEvent(): void
    {
        // JobFailed 事件：让 Hybrid/DLQ 模式的 handler 拿到异常做后续处理
        $events = $this->app->make('events');
        $events->listen(JobFailed::class, function (JobFailed $event) {
            try {
                $queue = $event->connectionName === 'kafka'
                    ? $this->app->make('kafka.manager')->connection()
                    : null;
                if ($queue === null) {
                    return;
                }
                $job = $event->job;
                // KafkaJob::fail() 已经自己处理了 failed handler 的分发
                // 此处仅占位，留给 v0.2 完善
                unset($job);
            } catch (\Throwable $e) {
                if ($this->app->bound(ExceptionHandler::class)) {
                    $handler = $this->app->make(ExceptionHandler::class);
                    $handler->report($e);
                }
            }
        });
    }

    /**
     * 注册 `kafka:work` 命令（仅 console 模式）。
     *
     * @return void
     */
    private function registerCommands(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                WorkCommand::class,
                DlqTailCommand::class,        // v0.3 Step 3
                ReplayCommand::class,         // v0.3 Step 4
                HorizonSnapshotCommand::class, // v0.4
            ]);
        }
    }

    /**
     * 暴露配置文件（业务方 `php artisan vendor:publish --tag=kafka-config`）。
     *
     * @return void
     */
    private function registerPublishing(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../config/kafka.php' => config_path('kafka.php'),
            ], 'kafka-config');
        }
    }

    /**
     * @return array<int, string>
     */
    public function provides(): array
    {
        return [
            KafkaManager::class,
            'kafka.manager',
            'queue.failer.kafka',
        ];
    }
}
