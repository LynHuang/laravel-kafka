<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;

/**
 * 在 `KafkaQueue::pushRaw` 调 `Producer::send` 成功后 dispatch。
 *
 * ## 业务方监听时机
 *
 * 想"消息已成功投递到 broker" → 监听本事件。
 *
 * ## 典型用途
 *
 * - 投递指标（业务方 QPS / payload 大小分布）
 * - 投递 trace span 标记
 * - post-publish 日志
 *
 * ## 触发点
 *
 * `KafkaQueue::pushRaw` 在 `dispatchEvent(MessagePublishing)` + `Producer::send()`
 * + `dispatchEvent(MessagePublished)` 流程最后一步同步 dispatch。
 *
 * @see \LaravelKafka\Events\MessagePublishing 投递前
 */
final class MessagePublished
{
    /**
     * 消息投递到的物理 topic。
     */
    private string $topic;

    /**
     * 消费侧包装的消息（含 header / payload）。
     */
    private Message $message;

    /**
     * @param string $topic  消息投递到的物理 topic
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
