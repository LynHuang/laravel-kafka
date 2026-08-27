<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;
use Throwable;

/**
 * 在 `DlqFailedJobHandler` / `HybridFailedJobHandler` 写完 DLQ topic 后 dispatch。
 *
 * ## 业务方监听时机
 *
 * 想"这条消息进 DLQ 了" → 监听本事件。
 *
 * ## 典型用途
 *
 * - DLQ 告警（业务方业务方业务场景下想知道哪条消息进 DLQ）
 * - trace span 标记 fatal
 * - 业务方业务方手动 replay 前的清理工作
 *
 * ## 触发点
 *
 * `DlqFailedJobHandler::handle` 调 `Producer::send` 到 DLQ topic 成功后同步 dispatch。
 */
final class MessageSentToDLQ
{
    /**
     * DLQ topic 名（来自 `kafka.connections.*.failed.dlq.topic`）。
     */
    private string $dlqTopic;

    /**
     * 消费侧包装的消息（含 header / payload）。
     */
    private Message $message;

    /**
     * 触发 DLQ 的原始异常。
     */
    private Throwable $error;

    /**
     * @param string $dlqTopic DLQ topic 名
     * @param Message $message 消费侧包装的消息
     * @param Throwable $error 触发 DLQ 的原始异常
     */
    public function __construct(
        string $dlqTopic,
        Message $message,
        Throwable $error
    ) {
        $this->dlqTopic = $dlqTopic;
        $this->message = $message;
        $this->error = $error;
    }

    /**
     * DLQ topic 名。
     */
    public function dlqTopic(): string
    {
        return $this->dlqTopic;
    }

    /**
     * 消息值对象。
     */
    public function message(): Message
    {
        return $this->message;
    }

    /**
     * 触发 DLQ 的原始异常。
     */
    public function error(): Throwable
    {
        return $this->error;
    }
}
