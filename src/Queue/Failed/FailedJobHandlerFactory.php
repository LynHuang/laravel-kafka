<?php

declare(strict_types=1);

namespace LaravelKafka\Queue\Failed;

use Illuminate\Contracts\Container\Container;
use Illuminate\Database\ConnectionResolverInterface;
use LaravelKafka\Config\KafkaConfig;
use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Producer\Producer;
use LaravelKafka\Producer\ProducerFactory;
use Ramsey\Uuid\Uuid;

/**
 * 失败处理器工厂（v0.1 单例工厂）。
 *
 * ## 角色
 *
 * 根据 `KafkaConfig::failed()['driver']` 选择具体 handler 实现：
 *  - `database` → {@see DatabaseFailedJobHandler}
 *  - `dlq` → {@see DlqFailedJobHandler}
 *  - `hybrid`（**默认**） → {@see HybridFailedJobHandler}
 *
 * 单 connection 单例缓存（`$config->name()` 为 key）。
 *
 * ## 业务方使用
 *
 * `KafkaManager::failedHandler()` 调本工厂，业务方**不**直接 `new`。
 * ServiceProvider 在 `queue.failer.kafka` 容器绑定也走本工厂。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 没有独立 Factory，handler 逻辑写在 `Manager` 类里；
 * 我们用独立 Factory + 容器注入，单测可以 mock 整个 factory。
 */
final class FailedJobHandlerFactory
{
    /**
     * Laravel 容器（拿 `ConnectionResolverInterface` + `ProducerFactory`）。
     */
    private Container $container;

    /**
     * 单例缓存（key = config name）。
     *
     * @var array<string, FailedJobHandlerInterface>
     */
    private array $instances = [];

    /**
     * @param Container $container Laravel 容器
     */
    public function __construct(Container $container)
    {
        $this->container = $container;
    }

    /**
     * 获取（或新建）失败处理器。
     *
     * ## 模式选择
     *
     * 读 `kafka.connections.{name}.failed.driver`：
     *  - `database` → `makeDatabase()` → `DatabaseFailedJobHandler`
     *  - `dlq` → `makeDlq()` → `DlqFailedJobHandler`
     *  - `hybrid` → `makeHybrid()` → `HybridFailedJobHandler`
     *
     * @param KafkaConfig $config
     * @return FailedJobHandlerInterface 单例
     * @throws KafkaException 非法 driver 字符串时
     */
    public function make(KafkaConfig $config): FailedJobHandlerInterface
    {
        $key = $config->name();
        if (isset($this->instances[$key])) {
            return $this->instances[$key];
        }

        $driver = (string) ($config->failed()['driver'] ?? 'hybrid');

        switch ($driver) {
            case 'database':
                $this->instances[$key] = $this->makeDatabase($config);
                break;
            case 'dlq':
                $this->instances[$key] = $this->makeDlq($config);
                break;
            case 'hybrid':
                $this->instances[$key] = $this->makeHybrid($config);
                break;
            default:
                throw new KafkaException(sprintf(
                    'Invalid failed.driver "%s". Allowed: database, dlq, hybrid.',
                    $driver
                ));
        }

        return $this->instances[$key];
    }

    /**
     * `make()` 的别名（兼容 ServiceProvider 的 `queue.failer.kafka` 容器绑定）。
     *
     * @param KafkaConfig $config
     * @return FailedJobHandlerInterface
     */
    public function makeFor(KafkaConfig $config): FailedJobHandlerInterface
    {
        return $this->make($config);
    }

    /**
     * 构造 Database handler。
     *
     * 读 `kafka.connections.{name}.failed.database`：
     *  - `table`（默认 `failed_jobs`）—— DB 表名
     *  - `connection`（默认 null）—— Laravel DB connection 名
     *
     * @param KafkaConfig $config
     * @return DatabaseFailedJobHandler
     */
    private function makeDatabase(KafkaConfig $config): DatabaseFailedJobHandler
    {
        $dbConfig = (array) ($config->failed()['database'] ?? []);
        $table = (string) ($dbConfig['table'] ?? 'failed_jobs');
        $connection = $dbConfig['connection'] ?? null;

        $database = $this->container->make(ConnectionResolverInterface::class)
            ->connection($connection);

        return new DatabaseFailedJobHandler($database, $table, Uuid::uuid4());
    }

    /**
     * 构造 DLQ handler。
     *
     * 读 `kafka.connections.{name}.failed.dlq`：
     *  - `topic`（默认空）—— 显式 DLQ topic
     *  - `auto_topic_suffix`（默认 `.dlq`）—— 兜底后缀
     *  - `message_truncate_bytes`（默认 4096）—— 异常 message 截断
     *  - `trace_truncate_bytes`（默认 32768）—— 异常 trace 截断
     *
     * @param KafkaConfig $config
     * @return DlqFailedJobHandler
     */
    private function makeDlq(KafkaConfig $config): DlqFailedJobHandler
    {
        $dlqConfig = (array) ($config->failed()['dlq'] ?? []);
        $producer = $this->container->make(ProducerFactory::class)->make($config);

        return new DlqFailedJobHandler(
            $producer,
            $this->resolveDlqTopic($config, $dlqConfig),
            $dlqConfig,
        );
    }

    /**
     * 构造 Hybrid handler。
     *
     * 读 `kafka.connections.{name}.failed.hybrid`：
     *  - `max_attempts`（默认 3）—— 重试上限
     *  - `fatal_exceptions`（默认 `[]`）—— 立即 DLQ 的异常类名列表
     *  - `trace_truncate_bytes`（默认 32768）—— 透传给 DLQ handler
     *  - `message_truncate_bytes`（默认 4096）—— 透传给 DLQ handler
     *
     * @param KafkaConfig $config
     * @return HybridFailedJobHandler
     */
    private function makeHybrid(KafkaConfig $config): HybridFailedJobHandler
    {
        $hybridConfig = (array) ($config->failed()['hybrid'] ?? []);

        return new HybridFailedJobHandler(
            $this->makeDatabase($config),
            $this->makeDlq($config),
            (int) ($hybridConfig['max_attempts'] ?? 3),
            (array) ($hybridConfig['fatal_exceptions'] ?? []),
            (int) ($hybridConfig['trace_truncate_bytes'] ?? 32768),
            (int) ($hybridConfig['message_truncate_bytes'] ?? 4096),
        );
    }

    /**
     * 解析 DLQ topic 名。
     *
     * 优先级：
     *  1. `$dlqConfig['topic']` 显式指定（业务方一次性覆盖）
     *  2. `$config->defaultTopic() . $dlqConfig['auto_topic_suffix']` 自动追加后缀
     *
     * @param KafkaConfig $config
     * @param array<string,mixed> $dlqConfig
     * @return string DLQ topic 名
     */
    private function resolveDlqTopic(KafkaConfig $config, array $dlqConfig): string
    {
        $explicit = (string) ($dlqConfig['topic'] ?? '');
        if ($explicit !== '') {
            return $explicit;
        }
        $suffix = (string) ($dlqConfig['auto_topic_suffix'] ?? '.dlq');
        return $config->defaultTopic() . $suffix;
    }
}
