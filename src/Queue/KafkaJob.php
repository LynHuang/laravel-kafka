<?php

declare(strict_types=1);

namespace LaravelKafka\Queue;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use LaravelKafka\Consumer\Consumer;
use LaravelKafka\Producer\Serializer\PhpSerializer;
use LaravelKafka\Producer\Serializer\Serializer;
use LaravelKafka\Queue\Failed\FailedContext;
use LaravelKafka\Queue\Failed\FailedJobHandlerInterface;
use LaravelKafka\Support\Header;
use LaravelKafka\Support\Str;
use Throwable;

/**
 * Kafka 消息的 Laravel Job 包装（v0.1）。
 *
 * ## 角色
 *
 * - 继承 `Illuminate\Queue\Jobs\Job` 拿到 `getName()` / `resolveName()` / `payload()` 等基类逻辑
 * - 持有 `RdKafka\Message` 原始引用（用于 ack offset）
 * - 实现 `fail()` 走 {@see FailedJobHandlerInterface}
 *
 * ## v0.1 vs v0.2
 *
 * v0.1：ack 逻辑在 `delete()` / `fail()` 里直接调 `$this->consumer->ack($rdMessage)`。
 * v0.2：ack 集中在 {@see Consumer::wrap()}（包装 HandlerResult 自动 commit）。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 把 ack 逻辑散在 Handler 里；
 * 我们集中在 KafkaJob + Consumer，业务方实现 Handler 时不用管 offset 细节。
 */
final class KafkaJob extends Job implements JobContract
{
    private Consumer $consumer;

    /**
     * librdkafka 原始消息（用于 ack / rebalance 跟踪）。
     *
     * @var \RdKafka\Message
     */
    private $rdMessage;

    /**
     * 标准化后的 Kafka headers（`array<string,string>`）。
     *
     * @var array<string,string>
     */
    private array $headers;

    /**
     * @param Container $container Laravel 容器
     * @param Consumer $consumer 当前 consumer（用于 ack）
     * @param \RdKafka\Message $rdMessage librdkafka 原始消息
     * @param string $connectionName Laravel connection 名
     * @param string $queue Laravel 逻辑队列名（即 Kafka topic 名）
     */
    public function __construct(
        Container $container,
        Consumer $consumer,
        \RdKafka\Message $rdMessage,
        string $connectionName,
        string $queue
    ) {
        $this->container = $container;
        $this->consumer = $consumer;
        $this->rdMessage = $rdMessage;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
        $this->rawBody = (string) $rdMessage->payload;
        $this->headers = $this->normalizeHeaders($rdMessage->headers);
    }

    /**
     * 拿 Job 唯一 id。
     *
     * 优先级：
     *  1. push 时注入的 `x-job-id` header（业务方显式）
     *  2. librdkafka `offset`（兜底）
     *
     * @return string|null job id
     */
    public function getJobId(): ?string
    {
        // 优先用 Header 里的 job id（push 时注入），否则用 offset
        $jobId = $this->headers[Header::JOB_ID] ?? null;
        if ($jobId !== null) {
            return $jobId;
        }
        return (string) $this->rdMessage->offset;
    }

    /**
     * 拿原始 payload（未反序列化）。
     *
     * @return string 原始字节串
     */
    public function getRawBody(): string
    {
        return $this->rawBody;
    }

    /**
     * 当前已尝试次数。
     *
     * 读 `x-retry-count` header（push 时初始为 0，requeue 时 +1）。
     * 返回 `int + 1`（attempts 从 1 开始计，符合 Laravel 语义）。
     *
     * @return int 当前尝试次数（1 = 首次）
     */
    public function attempts(): int
    {
        $count = $this->headers[Header::RETRY_COUNT] ?? '0';
        return (int) $count + 1;
    }

    /**
     * 单个 header 查询。
     *
     * @param string $name header 名
     * @param string|null $default 不存在时的默认值
     * @return string|null
     */
    public function header(string $name, ?string $default = null): ?string
    {
        return $this->headers[$name] ?? $default;
    }

    /**
     * 全部 headers（标准化后）。
     *
     * @return array<string,string>
     */
    public function headers(): array
    {
        return $this->headers;
    }

    /**
     * 拿 librdkafka 原始消息（业务方做高级操作时用，如直接读 timestamp）。
     *
     * @return \RdKafka\Message
     */
    public function rdMessage(): \RdKafka\Message
    {
        return $this->rdMessage;
    }

    /**
     * 释放 Job 回队列（v0.1 占位）。
     *
     * v0.1：实际 release 由 {@see \LaravelKafka\Consumer\Handler\NativeHandler}
     *       通过 `HandlerResult::requeue()` 路径处理（带 `x-retry-count` header 自增）。
     *
     * v0.2+ 计划：在本方法直接调 `KafkaQueue::pushRaw` 入队，避免双重入队。
     *
     * @param int $delay 延迟秒数（v0.1 占位）
     * @return void
     */
    public function release($delay = 0)
    {
        // 通知父类释放标记
        parent::release($delay);

        // 实际上 release 由 NativeHandler 通过 HandlerResult::requeue() 路径处理
        // 这里不直接 produce，避免双重入队
    }

    /**
     * 标记 Job 完成（提交 offset）。
     *
     * Laravel Queue 框架在 Job 处理成功后调本方法。
     * 我们**额外**调 `$this->consumer->ack($rdMessage)` 提交 offset。
     *
     * @return void
     */
    public function delete()
    {
        parent::delete();
        // ack offset
        $this->consumer->ack($this->rdMessage);
    }

    /**
     * 标记 Job 失败（走失败处理器）。
     *
     * ## 流程
     *
     *  1. `markAsFailed()` 通过反射设置父类 `failed = true`
     *  2. 拿 `FailedJobHandlerFactory::makeFor($config)` 拿失败处理器
     *  3. 构造 {@see FailedContext}（含 payload / headers / topic / partition / attempts）
     *  4. `handler->handle($this, $exception, $context)` —— 写 DB / DLQ
     *  5. 提交 offset（消息从主 topic 视角移除，去向由 handler 决定）
     *
     * ## 失败传播
     *
     * 失败 handler 自身抛异常 → 冒泡到 `NativeHandler` → 走 requeue（保命）。
     *
     * @param Throwable|null $exception 业务异常
     * @return void
     */
    public function fail($exception)
    {
        $this->markAsFailed();

        if ($exception instanceof Throwable) {
            $handler = $this->container->make(\LaravelKafka\Queue\Failed\FailedJobHandlerFactory::class)
                ->makeFor($this->container->make('kafka.manager')->config());

            $handler->handle($this, $exception, new FailedContext(
                $this->rawBody,
                $this->headers,
                (string) ($this->rdMessage->topic_name ?? 'laravel-jobs'),
                (int) ($this->rdMessage->partition ?? 0),
                $this->attempts() - 1,
            ));
        }

        // 提交 offset（消息已经从主 topic 视角移除了，去向由 handler 决定）
        $this->consumer->ack($this->rdMessage);
    }

    /**
     * 内部：反射设置父类 `failed = true`（Laravel Job 内部状态）。
     *
     * v0.1 父类 `failed` 是 private，**必须**用 reflection。v0.2 评估用 `with` 子类直接暴露。
     */
    private function markAsFailed(): void
    {
        // 通过 reflection 设置父类的 failed 标志
        $reflection = new \ReflectionClass(parent::class);
        if ($reflection->hasProperty('failed')) {
            $prop = $reflection->getProperty('failed');
            $prop->setAccessible(true);
            $prop->setValue($this, true);
        }
    }

    /**
     * 把 librdkafka headers 标准化成 `array<string,string>`。
     *
     * librdkafka 返回的 headers 可能是 `array<string,string>` 或空（v0.1 ext-rdkafka 行为），
     * 统一转 `string`。
     *
     * @param mixed $rdHeaders librdkafka headers 结构
     * @return array<string,string>
     */
    private function normalizeHeaders($rdHeaders): array
    {
        $out = [];
        if (! is_array($rdHeaders)) {
            return $out;
        }
        foreach ($rdHeaders as $k => $v) {
            $out[(string) $k] = (string) $v;
        }
        return $out;
    }
}
