<?php

declare(strict_types=1);

namespace LaravelKafka\Consumer\Handler;

/**
 * Handler 处理结果值对象（v0.1）。
 *
 * ## 角色
 *
 * 业务方 {@see HandlerInterface::handle()} 必须返回 `HandlerResult`，**不**抛异常决定后续动作。
 * 三个动作对应 Kafka 侧的语义：
 *
 * | 动作 | Kafka 侧行为 | 业务场景 |
 * | --- | --- | --- |
 * | `ack` | 提交 offset，消息从主 topic 移除 | 处理成功 |
 * | `requeue` | 重新入队到主 topic（增加 retry_count） | 临时错误（DB 短抖 / 第三方超时） |
 * | `dlq` | 写 DLQ topic 然后提交 offset | 永久错误（payload 非法 / 重试已超限） |
 *
 * ## 不可变 + named constructor
 *
 * 用 `HandlerResult::ack()` / `requeue($e)` / `dlq($e)` 三个 named constructor，
 * 业务方**不**直接 `new HandlerResult('ack')`（不直观）。
 *
 * ## error 字段语义
 *
 * | 动作 | error 字段 |
 * | --- | --- |
 * | `ack` | null（无错误） |
 * | `requeue` | 可选 Throwable（用于日志 / DLQ 元信息） |
 * | `dlq` | 必填 Throwable（DLQ payload 记录原始异常） |
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 直接 return `bool` + 抛异常区分 ack / fail，业务方易踩坑；
 * 我们用三个 named constructor + 类型安全枚举（PHP 7.4 兼容）。
 */
final class HandlerResult
{
    /**
     * 提交 offset，消息从主 topic 移除（处理成功）。
     */
    public const ACTION_ACK = 'ack';

    /**
     * 重新入队到主 topic（用于临时错误重试）。
     */
    public const ACTION_REQUEUE = 'requeue';

    /**
     * 写 DLQ topic 然后提交 offset（用于永久错误兜底）。
     */
    public const ACTION_DLQ = 'dlq';

    /**
     * 动作字符串（'ack' / 'requeue' / 'dlq'）。
     */
    private string $action;

    /**
     * 业务方传入的原始异常（ack 时 null）。
     */
    private ?\Throwable $error;

    /**
     * 处理成功：提交 offset。
     *
     * 业务方在 try 块正常返回时调用。
     *
     * @return self
     */
    public static function ack(): self
    {
        return new self(self::ACTION_ACK);
    }

    /**
     * 临时错误：重试（requeue 到主 topic）。
     *
     * `NativeHandler` 会把 `error` 记录到日志，并增加 `x-attempt` header 后重投。
     *
     * @param \Throwable|null $error 原始异常（可选，仅用于日志）
     * @return self
     */
    public static function requeue(\Throwable $error = null): self
    {
        return new self(self::ACTION_REQUEUE, $error);
    }

    /**
     * 永久错误：写 DLQ。
     *
     * `NativeHandler` 会把 `$error` + topic + headers 打包到 DLQ payload。
     *
     * @param \Throwable $error 原始异常（**必填**，写 DLQ payload 用）
     * @return self
     */
    public static function dlq(\Throwable $error): self
    {
        return new self(self::ACTION_DLQ, $error);
    }

    /**
     * @param string $action 三个 action 常量之一
     * @param \Throwable|null $error 原始异常（ack 时 null）
     * @throws \InvalidArgumentException 非法 action 字符串时
     */
    public function __construct(string $action, ?\Throwable $error = null)
    {
        if (! in_array($action, [self::ACTION_ACK, self::ACTION_REQUEUE, self::ACTION_DLQ], true)) {
            throw new \InvalidArgumentException(sprintf('Invalid handler action: %s', $action));
        }
        $this->action = $action;
        $this->error = $error;
    }

    /**
     * 拿动作字符串。
     *
     * @return string 'ack' / 'requeue' / 'dlq'
     */
    public function action(): string
    {
        return $this->action;
    }

    /**
     * 是否 ack。
     *
     * @return bool
     */
    public function isAck(): bool
    {
        return $this->action === self::ACTION_ACK;
    }

    /**
     * 是否 requeue。
     *
     * @return bool
     */
    public function isRequeue(): bool
    {
        return $this->action === self::ACTION_REQUEUE;
    }

    /**
     * 是否 dlq。
     *
     * @return bool
     */
    public function isDlq(): bool
    {
        return $this->action === self::ACTION_DLQ;
    }

    /**
     * 拿原始异常（ack 时 null）。
     *
     * @return \Throwable|null
     */
    public function error(): ?\Throwable
    {
        return $this->error;
    }
}
