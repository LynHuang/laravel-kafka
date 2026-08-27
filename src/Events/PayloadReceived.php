<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;

/**
 * 裸事件（非 Laravel Job）消费成功后 dispatch。
 *
 * ## 触发时机
 *
 * v0.5.0 起，`NativeHandler::handle()` 检测到消息**不是** Laravel Job payload
 * （没有 `data.command` 字段）时，按 `x-serializer` header 用对应 Serializer
 * 反序列化 payload，然后 dispatch 本事件。之后消息 ack（提交 offset）。
 *
 * ## 典型用途
 *
 * - 业务方用 `Producer::send` + `JsonSerializer` 直接发业务事件（跨语言消费场景），
 *   同一 `kafka:work` worker 监听本事件处理
 * - 非 Laravel Job 的消息（Node/Go/Python 发的自定义事件）
 *
 * ## 与 Laravel Job 的关系
 *
 * - Laravel Job（`Queue::push` / `dispatch`）→ 走 `Worker::process`，**不**触发本事件
 * - 裸事件（`Producer::send` 直接发）→ 触发本事件
 *
 * ## 业务方监听
 *
 * ```php
 * // app/Providers/EventServiceProvider.php
 * Event::listen(\LaravelKafka\Events\PayloadReceived::class, function ($event) {
 *     // $event->payload() = ['event' => 'order.created', 'id' => 123, ...]
 *     // $event->topic()   = 'order-events'
 * });
 * ```
 *
 * @see \LaravelKafka\Events\MessageConsumed Laravel Job 成功时
 * @see \LaravelKafka\Producer\Serializer\JsonSerializer 裸事件常用序列化器
 */
final class PayloadReceived
{
    /**
     * 消息来自的物理 topic（来自 header `x-original-topic`）。
     */
    private string $topic;

    /**
     * Serializer 解码后的 payload（数组 / 标量 / null）。
     *
     * @var mixed
     */
    private $payload;

    /**
     * 原始 Kafka 消息（含 payload / headers / key）。
     */
    private Message $message;

    /**
     * @param string $topic 消息来自的物理 topic
     * @param mixed $payload Serializer 解码后的 payload
     * @param Message $message 原始 Kafka 消息
     */
    public function __construct(
        string $topic,
        $payload,
        Message $message
    ) {
        $this->topic = $topic;
        $this->payload = $payload;
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
     * Serializer 解码后的 payload。
     *
     * @return mixed
     */
    public function payload()
    {
        return $this->payload;
    }

    /**
     * 原始 Kafka 消息（含 payload / headers / key）。
     */
    public function message(): Message
    {
        return $this->message;
    }
}
