<?php

declare(strict_types=1);

namespace LaravelKafka\Queue\Failed;

/**
 * 失败任务上下文值对象（v0.1）。
 *
 * ## 角色
 *
 * 把"消息处理失败"时需要的所有元信息打包成不可变值对象，
 * 避免 {@see FailedJobHandlerInterface::handle()} 的签名爆炸（不然要传 6 个参数）。
 *
 * ## 业务方使用
 *
 * 在 `DatabaseFailedJobHandler` / `DlqFailedJobHandler` 实现里通过
 * `$context->topic()` / `$context->attempts()` 拿到信息，写入 DB 或 DLQ payload。
 *
 * ## v0.1 vs v0.2
 *
 * v0.1：`attempts` 是简单 int（不解析 header）。
 * v0.2+ 评估：把 attempts 解析逻辑从 `NativeHandler` 下放到本类，提供 `decodedAttempts()` 方法。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 用 `array $context` 直接传，业务方 `$context['topic']` 容易拼错 key；
 * 我们用值对象 + 方法名，IDE 补全 + 重构安全。
 */
final class FailedContext
{
    /**
     * @param string                $rawPayload 原始消息体（未反序列化）
     * @param array<string,string>  $headers    原始 Kafka headers（含 traceparent / x-trace-id / x-attempt）
     * @param string                $topic      源 topic
     * @param int                   $partition  源 partition
     * @param int                   $attempts   已重试次数（0 = 首次失败）
     */
    public function __construct(
        string $rawPayload,
        array $headers,
        string $topic,
        int $partition,
        int $attempts
    ) {
        $this->rawPayload = $rawPayload;
        $this->headers = $headers;
        $this->topic = $topic;
        $this->partition = $partition;
        $this->attempts = $attempts;
    }

    /**
     * 原始消息体（未反序列化）。
     *
     * 业务方写到 DB / DLQ 时**必须**存原始 payload（含原始 headers 信息），
     * 不要存反序列化后的对象（业务方重跑时可能 Laravel Job 类已经改了）。
     *
     * @return string 原始字节串
     */
    public function rawPayload(): string
    {
        return $this->rawPayload;
    }

    /**
     * 原始 Kafka headers。
     *
     * 含业务方自定义的 traceparent / x-trace-id / x-attempt / x-enqueued-at 等。
     * 写 DLQ payload 时业务方可以转 JSON 存（便于回放时反查）。
     *
     * @return array<string,string> key = header name，value = header value
     */
    public function headers(): array
    {
        return $this->headers;
    }

    /**
     * 源 topic 名。
     *
     * @return string
     */
    public function topic(): string
    {
        return $this->topic;
    }

    /**
     * 源 partition 编号。
     *
     * @return int partition 编号（0-based）
     */
    public function partition(): int
    {
        return $this->partition;
    }

    /**
     * 已重试次数。
     *
     * 0 = 首次失败（从未重试过），1 = 第 1 次重试后失败，依此类推。
     * `HybridFailedJobHandler` 用 `attempts >= max_attempts` 决策是否走 DLQ。
     *
     * @return int
     */
    public function attempts(): int
    {
        return $this->attempts;
    }
}
