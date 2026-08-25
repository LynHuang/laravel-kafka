<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;

/**
 * `Producer::send` 成功返回（broker 已 ack）后 dispatch。
 *
 * ## 业务方监听时机
 *
 * 想"这条消息已经成功落地" → 监听本事件（与 {@see MessagePublishing} 配对）。
 *
 * ## 触发点
 *
 * `KafkaQueue::pushRaw` 在 `Producer::send` 之后同步 dispatch（fake 模式**不**dispatch）。
 *
 * ## 典型用途
 *
 * - metrics 计数（生产成功 / 失败的对比）
 * - 发布成功告警抑制（如果失败由 {@see MessageFailed} 单独告警）
 * - replication lag 监控（`Producer::send` 返回时已 ack，但 follower 同步可能未完成）
 *
 * @see \LaravelKafka\Events\MessagePublishing 配对事件
 * @see \LaravelKafka\Queue\KafkaQueue::pushRaw() 触发点
 */
final class MessagePublished
{
    /**
     * @param string $topic  物理 topic 名
     * @param Message $message 构造好的消息（含 payload / headers / key）
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
