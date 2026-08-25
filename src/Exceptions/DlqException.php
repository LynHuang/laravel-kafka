<?php

declare(strict_types=1);

namespace LaravelKafka\Exceptions;

use RuntimeException;

/**
 * DLQ 写入失败异常（v0.1 基础类）。
 *
 * ## 角色
 *
 * {@see \LaravelKafka\Queue\Failed\DlqFailedJobHandler} 写 DLQ topic 失败时抛出本异常，
 * 表示"最后的兜底也兜不住了"（DLQ topic 不存在 / broker down / quota exceeded）。
 *
 * ## 业务层处理
 *
 * - `NativeHandler` 捕获 `DlqException` 后**不**当失败处理，而是把 `HandlerResult` 转为 `requeue`
 * - 消息保留在主 topic，等下轮重试 → 等到 DLQ topic 恢复后再尝试写入
 *
 * ## 为什么 requeue 而不是 fail
 *
 * 主线逻辑：消息处理失败 → 写 DLQ 兜底。如果 DLQ 也写失败，**再让消息丢 = 数据丢失**。
 * 业务方**绝对不能**吞掉 DlqException 然后 return（否则会触发框架默认 `HandlerResult::fail`，
 * 丢掉消息）。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 没有专门的 DlqException，DLQ 失败会被当普通 KafkaException 处理
 * （结果：业务方容易 catch 不到，消息丢失）；我们独立成类 + 框架默认 requeue，更安全。
 */
class DlqException extends RuntimeException
{
}
