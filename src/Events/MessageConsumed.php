<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;

/**
 * 在 `NativeHandler::handle` 调 `Worker::process` 成功后 dispatch。
 *
 * ## 业务方监听时机
 *
 * 想"这条消息处理成功" → 监听本事件。
 *
 * ## 典型用途
 *
 * - trace span 结束（与 {@see MessageConsuming} 配对）
 * - metrics 计数（成功处理 QPS）
 * - post-processing 日志
 *
 * ## 触发点
 *
 * `NativeHandler::handle` 在 `Worker::process` 成功后同步 dispatch。
 *
 * @see \LaravelKafka\Events\MessageConsuming 处理前
 * @see \LaravelKafka\Events\MessageFailed 业务异常后
 */
final class MessageConsumed
{
    /**
     * 消息来自的物理 topic（来自 header `x-original-topic`）。
     */
    private string $topic;

    /**
     * 消费侧包装的消息（含 header / payload）。
     */
    private Message $message;

    /**
     * @param string $topic  消息来自的物理 topic（来自 header `x-original-topic`）
     * @param Message $message 消费侧包装的消息（含 header / payload）
     */
    public function __construct(
        string $topic,
        Message $message
    ) {
        $this->topic = $topic;
        $this->message = $message;
    }

    /**
     * 物理 topic 名。
     */
    public function topic(): string
    {
        return $this->topic;
    }

    /**
     * 消息值对象。
     */
    public function message(): Message
    {
        return $this->message;
    }
}
