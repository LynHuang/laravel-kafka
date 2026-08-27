<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;

/**
 * 在 `KafkaQueue::pushRaw` 调 `Producer::send` 之前 dispatch。
 *
 * ## 业务方监听时机
 *
 * 想"我准备发这条消息了" → 监听本事件。
 *
 * ## 典型用途
 *
 * - 投递 trace span 开始
 * - 业务方想在 send 前改 message（不推荐——应该传时改）
 *
 * ## 触发点
 *
 * `KafkaQueue::pushRaw` 在 fake 模式检查之前同步 dispatch。
 *
 * @see \LaravelKafka\Events\MessagePublished 投递后
 */
final class MessagePublishing
{
    /**
     * 消息准备投递到的物理 topic。
     */
    private string $topic;

    /**
     * 消费侧包装的消息（含 header / payload）。
     */
    private Message $message;

    /**
     * @param string $topic  消息准备投递到的物理 topic
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
