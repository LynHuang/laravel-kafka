<?php

declare(strict_types=1);

namespace LaravelKafka\Events;

use LaravelKafka\Producer\Message;
use Throwable;

/**
 * 失败消息写入 DLQ topic 后 dispatch。
 *
 * ## 触发条件
 *
 * - `failed.driver = dlq` 模式：每条失败消息都进 DLQ → 必触发
 * - `failed.driver = hybrid` 模式：致命异常 / 达 max_attempts 时进 DLQ → 部分触发
 * - `failed.driver = database` 模式：**不**写 DLQ → 不触发
 *
 * ## 与 {@see MessageFailed} 的顺序
 *
 * ```
 * 业务抛异常
 *   ↓
 * MessageFailed dispatched
 *   ↓
 * failedHandler.handle()  // 写表 + 写 DLQ
 *   ↓
 * MessageSentToDLQ dispatched (仅当 DLQ 实际写入)
 * ```
 *
 * ## 典型用途
 *
 * - DLQ 消费告警（"DLQ 里又堆积了 1000 条失败消息"）
 * - metrics 计数（按 topic / exception class 拆）
 * - 关联追踪（用 `dlqTopic()` 定位写到哪个 DLQ）
 *
 * @see \LaravelKafka\Queue\Failed\DlqFailedJobHandler 触发点
 */
final class MessageSentToDLQ
{
    /**
     * @param string    $dlqTopic 写入的 DLQ topic 名
     * @param Message   $message   原消息（payload 不重新序列化）
     * @param Throwable $error     业务抛出的异常
     */
    public function __construct(
        string $dlqTopic,
        Message $message,
        Throwable $error
    ) {
        $this->dlqTopic = $dlqTopic;
        $this->message = $message;
        $this->error = $error;
    }

    /**
     * DLQ topic 名。
     *
     * @return string
     */
    public function dlqTopic(): string
    {
        return $this->dlqTopic;
    }

    /**
     * 原消息值对象。
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
