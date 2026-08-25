<?php

declare(strict_types=1);

namespace LaravelKafka\Support;

/**
 * Kafka 消息 Header 常量集中地。
 *
 * 命名约定：`x-` 前缀 + 小写 + `-` 分隔。
 * 所有用到的 Header 都在这里定义，避免散落在代码里的魔法字符串。
 */
final class Header
{
    // ───── 基础元信息（push 时由 KafkaQueue 注入） ─────
    public const TRACE_ID = 'x-trace-id';
    public const QUEUE = 'x-queue';
    public const CONNECTION = 'x-connection';
    public const ENQUEUED_AT = 'x-enqueued-at';
    public const RETRY_COUNT = 'x-attempt';
    public const SERIALIZER = 'x-serializer';

    // ───── 延迟消息 ─────
    public const AVAILABLE_AT = 'x-available-at';

    // ───── Job 标识 ─────
    public const JOB_ID = 'x-job-id';

    // ───── 原始 Kafka 位置（消费时由 Consumer 注入） ─────
    public const ORIGINAL_TOPIC = 'x-original-topic';
    public const ORIGINAL_PARTITION = 'x-original-partition';
    public const ORIGINAL_OFFSET = 'x-original-offset';
    public const ORIGINAL_HEADERS = 'x-original-headers';

    // ───── DLQ 专属（写 DLQ 时由 DlqFailedJobHandler 注入） ─────
    public const FAILED_AT = 'x-failed-at';
    public const EXCEPTION_CLASS = 'x-exception-class';
    public const EXCEPTION_MESSAGE = 'x-exception-message';
    public const EXCEPTION_TRACE = 'x-exception-trace';

    // ───── 路由（v0.3 启用） ─────
    public const HANDLER = 'x-handler';
    public const CONSUMER_GROUP = 'x-consumer-group';

    // ───── v0.2 W3C Trace Context（RFC 0003 Step 5） ─────
    //
    // W3C Trace Context 格式参考 https://www.w3.org/TR/trace-context/
    // 完整 traceparent 头：00-<32hex trace-id>-<16hex parent-id>-<2hex flags>
    // example:             00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
    //
    // 业务方跨服务调用时透传整个 traceparent 头，下游解析后能继续派生子 span
    public const TRACEPARENT = 'traceparent';
    public const TRACESTATE = 'tracestate';

    private function __construct()
    {
        // 不可实例化
    }
}
