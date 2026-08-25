<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;

/**
 * `Worker::process` 成功返回（业务完成）后 dispatch。
 *
 * ## 业务方监听时机
 *
 * 想"这条消息已被成功处理" → 监听本事件（与 {@see MessageConsuming} 配对）。
 *
 * ## 典型用途
 *
 * - 业务 metrics 计数（每条成功处理的任务）
 * - 消费侧 trace span 结束
 * - 告警抑制（如果 {@see MessageFailed} 会单独告警）
 *
 * ## 触发点
 *
 * `NativeHandler::handle` 在 `Worker::process` 成功返回后同步 dispatch。
 *
 * @see \LaravelKafka\Events\MessageConsuming 配对事件
 */
final class MessageConsumed
{
    /**
     * @param string $topic 消息来自的物理 topic
     * @param Message $message 消费侧包装的消息
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
