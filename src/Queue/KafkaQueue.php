<?php

declare(strict_types=1);

namespace LaravelKafka\Queue;

use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use LaravelKafka\Config\KafkaConfig;
use LaravelKafka\Consumer\Consumer;
use LaravelKafka\Events\MessagePublished;
use LaravelKafka\Events\MessagePublishing;
use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Manager\KafkaManager;
use LaravelKafka\Producer\Message;
use LaravelKafka\Producer\Producer;
use LaravelKafka\Queue\Failed\FailedJobHandlerInterface;
use LaravelKafka\Support\Testing\FakeMessageStorage;
use LaravelKafka\Support\Header;
use LaravelKafka\Support\TraceContext;

/**
 * Kafka 队列实现。
 *
 * ## 角色
 *
 * - 继承 `Illuminate\Queue\Queue`：拿到 Laravel 全部标准语义（payload 构造 / Job 解析 / retry header）
 * - 持有 Producer / Consumer / FailedHandler
 * - 实现 `QueueContract` 标准方法：`size / push / pushRaw / later / pop`
 *
 * ## push/pushRaw 流程
 *
 *  1. `createPayload($job, ...)`（基类方法）→ 把 Laravel Job 序列化成字符串 payload
 *  2. `resolveTopicWithOptions()` → 解析最终物理 topic
 *  3. `buildMessage()` → 注入 5+ 个 header（traceparent / x-trace-id / queue / connection / ...）
 *  4. **fake 检查**：fake 模式时只写 storage，不真发
 *  5. 真发：dispatch `MessagePublishing` → `Producer::send` → dispatch `MessagePublished`
 *
 * ## 业务方调用
 *
 * ```php
 * Queue::push(new MyJob());                  // Laravel 标准
 * app('kafka.manager')->connection()         // 拿本类实例
 *     ->push($job, '', 'emails', 'user-1');  // v0.2 第 4 参数 key
 * ```
 */
final class KafkaQueue extends Queue implements QueueContract
{
    /**
     * 真实发送客户端（封装 librdkafka Conf + RdKafka\Producer）。
     *
     * @var Producer
     */
    private Producer $producer;

    /**
     * 真实消费客户端（封装 RdKafka\KafkaConsumer + rebalance 回调）。
     *
     * @var Consumer
     */
    private Consumer $consumer;

    /**
     * 失败处理器（v0.1 三模式：database / dlq / hybrid）。
     *
     * @var FailedJobHandlerInterface
     */
    private FailedJobHandlerInterface $failedHandler;

    /**
     * Kafka 配置值对象（broker / topic / 失败处理等）。
     *
     * @var KafkaConfig
     */
    private KafkaConfig $config;

    /**
     * 当前 connection 名（如 'default' / 'reports'）。
     *
     * 注意：父类 `Illuminate\Queue\Queue::$connectionName` 已经是 protected，
     * 本类**不**重新声明（避免 "must be protected or weaker" 错误）。
     *
     * @var string
     */
    // protected string $connectionName;  // 继承自父类，不重声明

    /**
     * 构造时注入所有依赖。
     *
     * 父类 `Illuminate\Queue\Queue` 构造器需要 Container——这里传 null，
     * 容器在 {@see setContainer()} 延迟注入。
     *
     * @param Producer $producer         真实生产者
     * @param Consumer $consumer         真实消费者
     * @param FailedJobHandlerInterface $failedHandler 失败处理器
     * @param KafkaConfig $config         Kafka 配置
     * @param string $connectionName      connection 名
     */
    public function __construct(
        Producer $producer,
        Consumer $consumer,
        FailedJobHandlerInterface $failedHandler,
        KafkaConfig $config,
        string $connectionName
    ) {
        // v0.1 老 bug 修复：父类 Illuminate\Queue\Queue 是 abstract，**没有**显式构造器。
        // 旧代码 `parent::__construct(null)` 在 PHP 7.4 上会抛 "Cannot call constructor"。
        // 容器通过 {@see setContainer()} 后续注入，不需要在构造器调 parent。
        $this->producer = $producer;
        $this->consumer = $consumer;
        $this->failedHandler = $failedHandler;
        $this->config = $config;
        $this->connectionName = $connectionName;
    }

    /**
     * 延迟注入容器（Laravel 在 ServiceProvider boot 之后调）。
     *
     * 业务方一般**不直接调**——Laravel 框架内部用。
     *
     * @param \Illuminate\Contracts\Container\Container $container
     * @return void
     */
    public function setContainer(\Illuminate\Contracts\Container\Container $container): void
    {
        $this->container = $container;
    }

    /**
     * 拿到队列"长度"（v0.1 占位实现）。
     *
     * ## v0.1 行为
     *
     * 永远返回 0。Kafka 没有原生"队列长度"概念。
     *
     * ## v0.2+ 计划
     *
     * 接 Kafka admin API（`getOffsetPositions` / `getWatermarkOffsets`）估算 lag：
     * - 高水位 - 已提交 offset = 未消费消息数
     * - 给 best-effort 估算值
     *
     * @param string|null $queue 队列名
     * @return int 0（v0.1）
     */
    public function size($queue = null): int
    {
        return 0;
    }

    /**
     * 把 Laravel Job 推入队列。
     *
     * v0.2 新增第 4 参数 `$key` 用于 partition 路由（同 key → 同 partition → 严格顺序）。
     *
     * ## 业务方使用
     *
     * ```php
     * $queue = app('kafka.manager')->connection();
     * $queue->push($job);                              // 不传 key (key=null, 轮询 partition)
     * $queue->push($job, '', 'emails', 'user-42');     // v0.2 加 key
     * ```
     *
     * @param mixed $job  Laravel Job 实例 / 类名 / 字符串 handler
     * @param mixed $data 透传给 Job 的 data 字段
     * @param string|null $queue Laravel 逻辑队列名
     * @param string|null $key  v0.2 partition 路由键（null = librdkafka 轮询）
     * @return mixed
     */
    public function push($job, $data = '', $queue = null, ?string $key = null)
    {
        $options = [];
        if ($key !== null) {
            $options['key'] = $key;
        }
        return $this->pushRaw(
            $this->createPayload($job, $this->connectionName, $data, $queue),
            $queue,
            $options
        );
    }

    /**
     * 底层 push：直接接收 payload 字符串 + options。
     *
     * Laravel Queue 框架内部走 `push()`，但业务方也能直接调 `pushRaw` 跳过 Job 抽象。
     *
     * ## 流程
     *
     *  1. `resolveTopicWithOptions()` 解析 topic
     *  2. `buildMessage()` 构造 Message
     *  3. **fake 检查**：fake 模式只写 storage
     *  4. 真发：dispatch `MessagePublishing` → `Producer::send` → dispatch `MessagePublished`
     *
     * @param string $payload  已序列化的 payload（PHP serialize / JSON / 自定义）
     * @param string|null $queue Laravel 逻辑队列名
     * @param array<string,mixed> $options 透传选项：
     *                                     - `topic` (v0.2)：显式覆盖物理 topic
     *                                     - `key` (v0.2)：partition 路由键
     *                                     - `traceparent` (v0.2)：透传 W3C Trace Context
     *                                     - `delay_seconds`：延迟消息秒数
     * @return int partition 编号（fake 模式返回 0）
     * @throws KafkaException Producer::send 失败时
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $topic = $this->resolveTopicWithOptions($queue, $options);
        $message = $this->buildMessage($payload, $options, $queue);

        if ($this->isFakeMode()) {
            /** @var FakeMessageStorage $storage */
            $storage = $this->container->make(FakeMessageStorage::class);
            $storage->record($topic, $message);
            return 0;
        }

        // 真发路径：dispatch 2 个事件（v0.2 引入）
        $this->dispatchEvent(new MessagePublishing($topic, $message));

        $partition = $this->producer->send($topic, $message);

        $this->dispatchEvent(new MessagePublished($topic, $message));

        return $partition;
    }

    /**
     * 内部：解析最终物理 topic（v0.2 增强）。
     *
     * 优先级（从高到低）：
     *  1. `options['topic']` 显式覆盖（业务方一次性指定）
     *  2. `KafkaConfig::topics[$queue]` 映射（业务方长期配置）
     *  3. 队列名当 topic（同名）
     *  4. `defaultTopic` 兜底
     *
     * @param string|null $queue Laravel 逻辑队列名
     * @param array<string,mixed> $options
     * @return string 物理 topic 名
     */
    private function resolveTopicWithOptions(?string $queue, array $options): string
    {
        if (isset($options['topic']) && $options['topic'] !== '') {
            return (string) $options['topic'];
        }
        return $this->resolveTopic($queue);
    }

    /**
     * 内部：dispatch Laravel 事件（容器未绑 Dispatcher 时静默跳过）。
     *
     * 为什么静默跳过：单元测试可能没有完整 Laravel 容器，业务方测试 KafkaFake 时
     * 不希望"缺 Dispatcher 绑定"导致 pushRaw 抛异常。
     *
     * @param object $event Laravel 事件实例
     * @return void
     */
    private function dispatchEvent(object $event): void
    {
        if ($this->container === null) {
            return;
        }
        if (! $this->container->bound(Dispatcher::class)) {
            return;
        }
        $this->container->make(Dispatcher::class)->dispatch($event);
    }

    /**
     * 内部：检查 fake 模式。
     *
     * 防御性检查：容器未注入 / KafkaManager 未绑定都返回 false（视为非 fake）。
     *
     * @return bool true = fake 模式
     */
    private function isFakeMode(): bool
    {
        if ($this->container === null) {
            return false;
        }
        if (! $this->container->bound(KafkaManager::class)) {
            return false;
        }
        return $this->container->make(KafkaManager::class)->isFake();
    }

    /**
     * 延迟消息 push（v0.1 占位 + v0.2 增强）。
     *
     * v0.1：通过 `options['delay_seconds']` 写到 `x-available-at` header，
     *       消费端 NativeHandler 同步阻塞等待。
     * v0.2+：走时间轮分层 topic（见 RFC 0003 v0.3 计划）。
     *
     * @param int $delay 延迟秒数
     * @param mixed $job
     * @param mixed $data
     * @param string|null $queue
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->pushRaw(
            $this->createPayload($job, $this->connectionName, $data, $queue),
            $queue,
            ['delay_seconds' => max(0, (int) $delay)]
        );
    }

    /**
     * 同步 pop（v0.1 占位：永远返回 null）。
     *
     * Kafka 模型与 Laravel 同步 pop 循环不兼容——真正的 pop 在
     * `kafka:work` 长驻进程的 poll 循环里。
     *
     * 返回 null 让 Laravel 默认 worker 拿到 null 直接退出（不消费任何 Kafka 消息）。
     *
     * @param string|null $queue
     * @return null 永远返回 null
     */
    public function pop($queue = null)
    {
        return null;
    }

    /**
     * 拿 KafkaConfig 值对象。
     *
     * @return KafkaConfig
     */
    public function config(): KafkaConfig
    {
        return $this->config;
    }

    /**
     * 拿 connection 名。
     *
     * @return string
     */
    public function connectionName(): string
    {
        return $this->connectionName;
    }

    /**
     * 拿失败处理器（v0.1 三模式之一）。
     *
     * @return FailedJobHandlerInterface
     */
    public function failedHandler(): FailedJobHandlerInterface
    {
        return $this->failedHandler;
    }

    /**
     * 内部：解析 topic（v0.1 已有 `KafkaConfig::resolveTopic`）。
     *
     * @param string|null $queue Laravel 逻辑队列名
     * @return string 物理 topic 名
     */
    private function resolveTopic(?string $queue): string
    {
        return $this->config->resolveTopic($queue);
    }

    /**
     * 内部：构造带 5+ header 的 Message。
     *
     * ## Header 清单
     *
     * | Header | 用途 |
     * | --- | --- |
     * | `traceparent` (v0.2) | W3C Trace Context 32hex trace-id |
     * | `x-trace-id` (v0.1 + v0.2) | 16hex 短 id，保留 v0.1 兼容 |
     * | `x-queue` | Laravel 逻辑队列名 |
     * | `x-connection` | connection 名 |
     * | `x-enqueued-at` | 入队时间戳 (ms) |
     * | `x-attempt` | 重试计数（0 = 首次） |
     * | `x-serializer` | 序列化器标识（默认 'php'） |
     * | `x-available-at` (可选) | 延迟消息到期时间戳 |
     *
     * @param string $payload 已序列化的 payload
     * @param array<string,mixed> $options
     * @param string|null $queue Laravel 逻辑队列名
     * @return Message
     */
    private function buildMessage(string $payload, array $options, ?string $queue): Message
    {
        $now = (int) (microtime(true) * 1000);

        // v0.2 引入：W3C Trace Context（traceparent）+ 向后兼容 x-trace-id
        $traceparent = $options['traceparent'] ?? TraceContext::next();
        $shortId = TraceContext::shortTraceId($traceparent) ?? bin2hex(random_bytes(8));

        $headers = [
            Header::TRACEPARENT => $traceparent,
            Header::TRACE_ID => $shortId,
            Header::QUEUE => (string) ($queue ?? $this->config->defaultTopic()),
            Header::CONNECTION => $this->connectionName,
            Header::ENQUEUED_AT => (string) $now,
            Header::RETRY_COUNT => '0',
            Header::SERIALIZER => 'php',
        ];

        if (isset($options['delay_seconds']) && (int) $options['delay_seconds'] > 0) {
            $headers[Header::AVAILABLE_AT] = (string) ($now + ((int) $options['delay_seconds']) * 1000);
        }

        return new Message(
            $payload,
            $headers,
            isset($options['key']) ? (string) $options['key'] : null,
        );
    }
}
