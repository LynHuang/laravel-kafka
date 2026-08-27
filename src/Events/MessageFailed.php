<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;
use Throwable;

/**
 * 在 `NativeHandler::handle` 调 `Worker::process` 抛异常后 dispatch（在写 DLQ 之前）。
 *
 * ## 业务方监听时机
 *
 * 想"这条消息处理失败" → 监听本事件。
 *
 * ## 典型用途
 *
 * - trace span 标记 error（与 {@see MessageConsuming} 配对）
 * - 失败告警 / 告警通知
 * - 业务方决定 requeue / DLQ / 静默
 *
 * ## 触发点
 *
 * `NativeHandler::handle` 在 `Worker::process` 抛异常时同步 dispatch。
 * 之后会调 {@see \LaravelKafka\Queue\Failed\FailedJobHandlerInterface::handle()}
 * 走 database / dlq / hybrid 处理。
 *
 * @see \LaravelKafka\Events\MessageConsuming 处理前
 * @see \LaravelKafka\Events\MessageConsumed 成功处理后
 */
final class MessageFailed
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
     * 原始异常。
     */
    private Throwable $error;

    /**
     * @param string $topic  消息来自的物理 topic
     * @param Message $message 消费侧包装的消息
     * @param Throwable $error 业务方 handle() 抛出的异常
     */
    public function __construct(
        string $topic,
        Message $message,
        Throwable $error
    ) {
        $this->topic = $topic;
        $this->message = $message;
        $this->error = $error;
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

    /**
     * 原始异常。
     */
    public function error(): Throwable
    {
        return $this->error;
    }
}
