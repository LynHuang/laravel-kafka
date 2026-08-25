<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;

/**
 * 在调用 `KafkaQueue::pushRaw` 之后、`Producer::send` 之前 dispatch。
 *
 * ## 业务方监听时机
 *
 * - 想"我准备发这条消息了" → 监听本事件
 * - 想"这条消息已经成功落地" → 监听 {@see MessagePublished}
 *
 * ## 典型用途
 *
 * - metrics 计数（消息生产前 / 生产后分别埋点）
 * - trace span 开始（与 {@see MessagePublished} 配对）
 * - 敏感字段过滤（mask 信用卡号等）
 * - 审计日志
 *
 * ## 业务方使用
 *
 * ```php
 * use LaravelKafka\Events\MessagePublishing;
 * use Illuminate\Support\Facades\Event;
 *
 * Event::listen(MessagePublishing::class, function (MessagePublishing $event) {
 *     Log::info('publishing', [
 *         'topic' => $event->topic(),
 *         'payload_size' => strlen($event->message()->payload()),
 *     ]);
 * });
 * ```
 *
 * @see \LaravelKafka\Events\MessagePublished 配对事件
 * @see \LaravelKafka\Queue\KafkaQueue::pushRaw() 触发点
 */
final class MessagePublishing
{
    /**
     * 物理 topic 名（已解析过 `KafkaConfig::resolveTopic`）。
     *
     * @param string $topic
     */
    public function __construct(
        string $topic,
        Message $message
    ) {
        $this->topic = $topic;
        $this->message = $message;
    }

    /**
     * 拿到物理 topic 名。
     *
     * @return string
     */
    public function topic(): string
    {
        return $this->topic;
    }

    /**
     * 拿到消息值对象（含 payload / headers / key）。
     *
     * @return Message
     */
    public function message(): Message
    {
        return $this->message;
    }
}
