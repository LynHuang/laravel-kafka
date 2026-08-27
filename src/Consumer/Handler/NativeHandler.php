<?php

declare(strict_types=1);

namespace LaravelKafka\Consumer\Handler;

use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use LaravelKafka\Events\MessageConsumed;
use LaravelKafka\Events\MessageConsuming;
use LaravelKafka\Events\MessageFailed;
use LaravelKafka\Events\PayloadReceived;
use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Horizon\HorizonMetricsRecorder;
use LaravelKafka\Producer\Message;
use LaravelKafka\Producer\Serializer\PhpSerializer;
use LaravelKafka\Producer\Serializer\Serializer;
use LaravelKafka\Queue\Failed\FailedJobHandlerInterface;
use LaravelKafka\Queue\KafkaJob;
use LaravelKafka\Queue\KafkaQueue;
use LaravelKafka\Support\Header;
use Throwable;

/**
 * Laravel Job 原生 handler（v0.1 唯一的 Handler）。
 *
 * ## 流程
 *
 *  1. 从 Message 拿 headers（x-queue / x-attempt / x-original-topic 等）
 *  2. 反序列化 payload（`PhpSerializer` / `JsonSerializer`）
 *  3. 实例化 `KafkaJob`（伪 `RdKafka\Message` 包装）
 *  4. 调 `Worker::process` 跑 Laravel Job
 *  5. 根据 process 抛异常 / 业务结果返回 `HandlerResult`
 *
 * ## v0.2 事件
 *
 * 在 `Worker::process` 前后 dispatch 三个事件：
 *  - `MessageConsuming`：处理前
 *  - `MessageConsumed`：处理成功后
 *  - `MessageFailed`：处理抛异常后（在写 DLQ 之前）
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 让业务方传 `Handler` 接口（callable / 闭包）；
 * 我们固定走 Laravel `Worker::process`，业务方继续用标准 Laravel Job。
 */
final class NativeHandler implements HandlerInterface
{
    /**
     * Laravel 容器（用于解析其他依赖）。
     *
     * @var Container
     */
    private Container $container;

    /**
     * Laravel Worker（封装 Worker::process 调用）。
     *
     * @var Worker
     */
    private Worker $worker;

    /**
     * 失败处理器（v0.1 三模式：database / dlq / hybrid）。
     *
     * @var FailedJobHandlerInterface
     */
    private FailedJobHandlerInterface $failedHandler;

    /**
     * Payload 反序列化器（默认 PhpSerializer）。
     *
     * v0.5.0 起真正被读：{@see resolveSerializer()} 懒加载到 serializers registry
     * （php → 本字段, json → JsonSerializer）。裸事件（非 Laravel Job）按
     * `x-serializer` header 选对应序列化器解码。
     *
     * @var Serializer
     */
    private Serializer $serializer;

    /**
     * 序列化器 registry（name → Serializer 实例）。
     *
     * v0.5.0 新增：默认含 `php` + `json`，业务方用 {@see registerSerializer()}
     * 注册自定义（如 avro）。裸事件消费时按 `x-serializer` header 选。
     *
     * @var array<string, Serializer>|null null = 未初始化（懒加载）
     */
    private ?array $serializers = null;

    /**
     * 构造时注入所有依赖。
     *
     * @param Container $container       Laravel 容器
     * @param Worker $worker             Laravel Worker
     * @param FailedJobHandlerInterface $failedHandler 失败处理器
     * @param Serializer $serializer     payload 反序列化器
     */
    public function __construct(
        Container $container,
        Worker $worker,
        FailedJobHandlerInterface $failedHandler,
        Serializer $serializer
    ) {
        $this->container = $container;
        $this->worker = $worker;
        $this->failedHandler = $failedHandler;
        $this->serializer = $serializer;
    }

    /**
     * 处理一条 Kafka 消息（Handler 入口）。
     *
     * ## 流程
     *
     *  1. 从容器拿默认 `KafkaQueue`
     *  2. 构造 `KafkaJob`（伪 `RdKafka\Message` 包装）
     *  3. 从 config 读 max_attempts，构造 `WorkerOptions`
     *  4. dispatch `MessageConsuming` 事件
     *  5. `Worker::process($connectionName, $job, $options)` 跑 Laravel Job
     *  6. 成功 → dispatch `MessageConsumed` + 返回 `HandlerResult::ack()`
     *  7. 异常 → dispatch `MessageFailed` + 调 `onException()` 决定 ack/requeue/dlq
     *
     * @param Message $message 消费侧包装的消息（含 payload / headers / key）
     * @return HandlerResult 三态之一：ack / requeue / dlq
     * @throws KafkaException 默认连接不是 KafkaQueue 实例时
     */
    public function handle(Message $message): HandlerResult
    {
        // v0.4.1 hotfix: 必须在 try 块外声明 $startMs, 否则 catch 里 recordHorizonMetrics($message, $startMs, false) 触发
        // "Undefined variable: startMs" (handle() 没声明, 只有 try/catch 后用). 重构 v0.4 时漏的变量.
        $startMs = microtime(true);

        $queue = $this->container->make('kafka.manager')->connection();
        if (! $queue instanceof KafkaQueue) {
            throw new KafkaException('Default queue is not a KafkaQueue instance.');
        }

        // v0.5.0: 裸事件检测 —— 非 Laravel Job payload (无 data.command) 走 Serializer 路径
        if (! $this->isLaravelJobPayload($message->payload())) {
            return $this->handleRawPayload($message, $queue, $startMs);
        }

        $job = $this->createJob($message, $queue);

        $maxAttempts = (int) ($this->container->make('config')->get(
            'kafka.connections.default.failed.hybrid.max_attempts',
            3
        ));

        $options = new WorkerOptions(
            'kafka-default', // name
            0, // backoff (v0.4.8: 改 int, 之前是 '0' 字符串 phpstan 报 type mismatch)
            128, // memory
            60, // timeout
            1, // sleep
            $maxAttempts, // maxTries
            false, // force
            false, // stopWhenEmpty
            0, // maxJobs
            0, // maxTime
            0 // rest
        );

        try {
            $topic = $message->header(Header::ORIGINAL_TOPIC) ?? 'laravel-jobs';
            $this->dispatchEvent(new MessageConsuming($topic, $message));

            $this->worker->process(
                $this->container->make('config')->get('queue.default', 'kafka'),
                $job,
                $options
            );

            $this->dispatchEvent(new MessageConsumed($topic, $message));

            // v0.4: Horizon 兼容 — 记录 queue + job metrics
            $this->recordHorizonMetrics($message, $startMs, true);

            return HandlerResult::ack();
        } catch (Throwable $e) {
            $this->dispatchEvent(new MessageFailed(
                $message->header(Header::ORIGINAL_TOPIC) ?? 'laravel-jobs',
                $message,
                $e
            ));
            $this->recordHorizonMetrics($message, $startMs, false);
            return $this->onException($job, $message, $e);
        }
    }

    /**
     * 注册自定义序列化器（v0.5.0，兑现 docs/11-Serializer.md §4 承诺）。
     *
     * 裸事件（非 Laravel Job）消费时按 `x-serializer` header 选序列化器解码。
     * 内置 `php`（PhpSerializer）+ `json`（JsonSerializer）。
     *
     * @param string $name `x-serializer` header 值（如 'avro'）
     * @param Serializer $serializer 序列化器实现
     * @return void
     */
    public function registerSerializer(string $name, Serializer $serializer): void
    {
        $this->resolveSerializer(null); // 确保默认 registry 已初始化
        $this->serializers[$name] = $serializer;
    }

    /**
     * 内部：判断 payload 是否是 Laravel Job 格式。
     *
     * Laravel `Queue::createPayload` 输出 JSON：`{"uuid":..., "job":"...@call",
     * "data":{"commandName":..., "command":"O:..."}}`。`data.command` 存在 =
     * Laravel Job；否则 = 裸事件（非 Job）。
     *
     * @param string $raw payload 原始字符串
     * @return bool true = Laravel Job
     */
    private function isLaravelJobPayload(string $raw): bool
    {
        $decoded = json_decode($raw, true);
        return is_array($decoded)
            && isset($decoded['job'])
            && isset($decoded['data']['command']);
    }

    /**
     * 内部：处理裸事件（非 Laravel Job）。
     *
     * ## 流程
     *
     *  1. 按 `x-serializer` header resolve Serializer
     *  2. `Serializer::decode()` 解码 payload
     *  3. dispatch `PayloadReceived` 事件（业务方监听处理）
     *  4. commit offset（裸事件无 KafkaJob，直接提交当前消费位置）
     *  5. 解码失败 → dispatch `MessageFailed` + 复用 `onException`（requeue/dlq）
     *
     * @param Message $message 消费侧包装的消息
     * @param KafkaQueue $queue 默认 KafkaQueue 实例
     * @param float $startMs 处理开始时间戳（ms）
     * @return HandlerResult
     */
    private function handleRawPayload(Message $message, KafkaQueue $queue, float $startMs): HandlerResult
    {
        $topic = $message->header(Header::ORIGINAL_TOPIC) ?? 'laravel-jobs';

        try {
            // v0.5.0 配置化: header 缺失时传 null → resolveSerializer 读配置默认 (不写死 'php')
            $serializer = $this->resolveSerializer($message->header(Header::SERIALIZER));
            $decoded = $serializer->decode($message->payload());

            $this->dispatchEvent(new PayloadReceived($topic, $decoded, $message));
            $this->recordHorizonMetrics($message, $startMs, true);

            // ack 副作用：裸事件无 KafkaJob::delete()，直接提交当前 consumer offset
            $consumer = $this->container->make(\LaravelKafka\Consumer\Consumer::class);
            $consumer->commitAsync();

            return HandlerResult::ack();
        } catch (Throwable $e) {
            $this->dispatchEvent(new MessageFailed($topic, $message, $e));
            $this->recordHorizonMetrics($message, $startMs, false);

            // 复用 Laravel Job 的 requeue/dlq 决策（构造 KafkaJob 给 failedHandler）
            $job = $this->createJob($message, $queue);
            return $this->onException($job, $message, $e);
        }
    }

    /**
     * 内部：按 `x-serializer` header 选序列化器（v0.5.0）。
     *
     * 懒加载默认 registry：`php` → 构造注入的 $serializer（PhpSerializer），
     * `json` → 新 JsonSerializer。业务方用 {@see registerSerializer()} 扩展。
     *
     * @param string|null $name `x-serializer` header 值（null = 默认 php）
     * @return Serializer 匹配的序列化器（未匹配 fallback 到构造注入的默认）
     */
    private function resolveSerializer(?string $name): Serializer
    {
        if ($this->serializers === null) {
            $this->serializers = [
                'php' => $this->serializer,
                'json' => new \LaravelKafka\Producer\Serializer\JsonSerializer(),
            ];
        }
        if ($name === null) {
            // v0.5.0 配置化：裸事件无 x-serializer header 时用 config 默认
            $name = (string) $this->container->make('config')->get(
                'kafka.connections.default.serializer',
                'php'
            );
        }
        return $this->serializers[$name] ?? $this->serializer;
    }

    /**
     * 内部：v0.4 记录 Horizon metrics（成功 + 失败都记录，保持 Horizon 与原版语义一致）。
     *
     * - 成功：throughput +1，runtime 加权平均
     * - 失败：throughput 仍然 +1（业务方看到的是"任务处理尝试数"），runtime 也算上
     *
     * 如果容器没绑 `HorizonMetricsRecorder` → 静默跳过（业务方没启用 horizon metrics）。
     *
     * @param Message $message 消费侧包装的消息
     * @param float $startMs 处理开始时间戳（ms）
     * @param bool $success 是否成功
     * @return void
     */
    private function recordHorizonMetrics(Message $message, float $startMs, bool $success): void
    {
        if (! $this->container->bound(HorizonMetricsRecorder::class)) {
            return;
        }
        try {
            /** @var HorizonMetricsRecorder $recorder */
            $recorder = $this->container->make(HorizonMetricsRecorder::class);

            $runtimeMs = (microtime(true) * 1000) - $startMs;
            $topic = (string) ($message->header(Header::ORIGINAL_TOPIC) ?? 'laravel-jobs');

            $recorder->incrementQueue($topic, $runtimeMs);
        } catch (\Throwable $e) {
            // 静默：metrics 失败不应影响业务处理
            error_log('[laravel-kafka] Horizon metrics record failed: ' . $e->getMessage());
        }
    }

    /**
     * 内部：dispatch Laravel 事件（容器未绑 Dispatcher 时静默跳过）。
     *
     * @param object $event Laravel 事件实例
     * @return void
     */
    private function dispatchEvent(object $event): void
    {
        if (! $this->container->bound(Dispatcher::class)) {
            return;
        }
        $this->container->make(Dispatcher::class)->dispatch($event);
    }

    /**
     * 内部：构造 `KafkaJob`（v0.1 内部用）。
     *
     * 业务方**不直接调**。
     *
     * 构造一个伪 `RdKafka\Message` 实例喂给 `KafkaJob` 构造器。
     * 注意：`RdKafka\Message` 的 `headers` 公共属性在某些版本是只读，
     * librdkafka 升级时这里需要回归。
     *
     * @param Message $message 消费侧包装的消息
     * @param KafkaQueue $queue 默认 KafkaQueue 实例
     * @return KafkaJob
     */
    private function createJob(Message $message, KafkaQueue $queue): KafkaJob
    {
        $consumer = $this->container->make(\LaravelKafka\Consumer\Consumer::class);

        $raw = $message->payload();
        $headers = $message->headers();

        // 构造一个伪 RdKafka\Message 给 KafkaJob
        $rdMsg = new \RdKafka\Message();
        $rdMsg->payload = $raw;
        $rdMsg->headers = $headers;
        $rdMsg->key = $message->key();
        $rdMsg->partition = $message->partition() ?? 0;
        $rdMsg->offset = (int) ($headers['x-offset'] ?? 0);
        $rdMsg->topic_name = $message->header('x-original-topic') ?? 'laravel-jobs';

        $queueName = (string) ($headers['x-queue'] ?? 'default');
        $connectionName = (string) ($headers['x-connection'] ?? 'kafka');

        // v0.4.8: KafkaJob 构造接 \Illuminate\Container\Container (具体类).
        // $this->container 是 Laravel Container 实例, 实际是 Illuminate\Container\Container.
        // 但类型声明是 Contract\Container, 加 cast 满足类型检查.
        return new KafkaJob(
            /** @phpstan-ignore-next-line */
            $this->container,
            $consumer,
            $rdMsg,
            $connectionName,
            $queueName,
        );
    }

    /**
     * 内部：业务异常处理决策。
     *
     * ## 决策树
     *
     *  1. `failed.driver = 'database'` → 直接 ack（让 Laravel 默认 failed_jobs 流程处理）
     *  2. `failed.driver = 'dlq' / 'hybrid'` → 调 failedHandler.handle() 写库 / DLQ
     *  3. 致命异常 OR 达 max_attempts → 返回 dlq
     *  4. 其他 → 返回 requeue（重试）
     *
     * @param KafkaJob $job 失败的 Job
     * @param Message $message 失败的消息
     * @param Throwable $e 业务异常
     * @return HandlerResult
     */
    private function onException(KafkaJob $job, Message $message, Throwable $e): HandlerResult
    {
        // Laravel Worker 内部已经通过 JobFailed 事件把任务写进 failed_jobs（database 模式）
        // 这里我们额外处理 dlq / hybrid 模式下的 DLQ 写入
        $driver = (string) $this->container->make('config')->get(
            'kafka.connections.default.failed.driver',
            'hybrid'
        );

        if ($driver === 'database') {
            // 纯 database 模式：让 Laravel 默认流程处理
            return HandlerResult::ack();
        }

        // dlq / hybrid 模式：抛给 failedHandler
        $this->failedHandler->handle(
            $job,
            $e,
            new \LaravelKafka\Queue\Failed\FailedContext(
                $message->payload(),
                (array) $message->headers(),
                $message->header('x-original-topic') ?? 'laravel-jobs',
                $message->partition() ?? 0,
                (int) ($message->header('x-attempt') ?? 0),
            )
        );

        // 致命异常 → 直接 DLQ；其他 → 看是否到 maxAttempts
        $isFatal = $this->isFatalException($e);
        $attempt = (int) ($message->header('x-attempt') ?? 0) + 1;
        $maxAttempts = (int) $this->container->make('config')->get(
            'kafka.connections.default.failed.hybrid.max_attempts',
            3
        );

        if ($isFatal || $attempt >= $maxAttempts) {
            return HandlerResult::dlq($e);
        }

        return HandlerResult::requeue($e);
    }

    /**
     * 内部：判断异常是否在 fatal 列表。
     *
     * fatal 列表从 config `kafka.connections.default.failed.hybrid.fatal_exceptions` 读。
     * 业务方配置：例如 `[\LaravelKafka\Exceptions\SerializationException::class]`。
     *
     * @param Throwable $e 业务抛出的异常
     * @return bool true = 致命（直接 DLQ，不再重试）
     */
    private function isFatalException(Throwable $e): bool
    {
        $fatal = (array) $this->container->make('config')->get(
            'kafka.connections.default.failed.hybrid.fatal_exceptions',
            []
        );
        foreach ($fatal as $class) {
            if ($e instanceof $class) {
                return true;
            }
        }
        return false;
    }
}
