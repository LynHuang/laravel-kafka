<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;

/**
 * 在 `NativeHandler::handle` 调 `Worker::process` 之前 dispatch。
 *
 * ## 业务方监听时机
 *
 * 想"我准备处理这条消息了" → 监听本事件。
 *
 * ## 典型用途
 *
 * - trace span 开始（与 {@see MessageConsumed} / {@see MessageFailed} 配对）
 * - metrics 计数（消费侧 QPS）
 * - pre-processing 日志
 * - 在业务处理前加锁（用 `jobId` 去重防止并发）
 *
 * ## 触发点
 *
 * `NativeHandler::handle` 在 `Worker::process` 之前同步 dispatch。
 *
 * @see \LaravelKafka\Events\MessageConsumed 成功处理后
 * @see \LaravelKafka\Events\MessageFailed 业务异常后
 */
final class MessageConsuming
{
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
     *
     * @return string
     */
    public function topic(): string
    {
        return $this->topic;
    }

    /**
     * 消息值对象。
     *
     * @return Message
     */
    public function message(): Message
    {
        return $this->message;
    }
}
