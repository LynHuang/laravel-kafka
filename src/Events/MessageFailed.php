<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;
use Throwable;

/**
 * 业务处理抛出异常后 dispatch（在 failedHandler 写库 / DLQ 之前）。
 *
 * ## 业务方监听时机
 *
 * 想"这条消息处理失败了" → 监听本事件。
 *
 * ## 与 {@see MessageSentToDLQ} 的区别
 *
 * | 事件 | 触发点 | 用途 |
 * | --- | --- | --- |
 * | `MessageFailed` | 业务抛异常**立即** | 业务侧告警 / 异常脱敏 / metrics |
 * | `MessageSentToDLQ` | DLQ 写入**完成后** | DLQ 消费告警 / 关联追踪 |
 *
 * `MessageFailed` 一定触发（业务异常），`MessageSentToDLQ` **可能**触发
 * （database 模式不写 DLQ）。
 *
 * ## 典型用途
 *
 * - 业务告警（"订单处理失败率超阈值"）
 * - 失败 metrics（按 exception class 拆）
 * - 敏感异常脱敏（mask 错误信息里的手机号 / 身份证）
 *
 * @see \LaravelKafka\Events\MessageSentToDLQ DLQ 写入后
 */
final class MessageFailed
{
    /**
     * @param string    $topic   消息来自的物理 topic
     * @param Message   $message 消费侧包装的消息
     * @param Throwable $error   业务抛出的异常
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

    /**
     * 业务抛出的异常对象。
     *
     * @return Throwable
     */
    public function error(): Throwable
    {
        return $this->error;
    }
}
