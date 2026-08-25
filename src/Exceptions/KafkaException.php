<?php

declare(strict_types=1);

namespace LaravelKafka\Exceptions;

use RuntimeException;

/**
 * Kafka 操作通用异常（v0.1 基础类）。
 *
 * ## 角色
 *
 * 所有 laravel-kafka 内部对 librdkafka 的调用失败（produce / consume / admin / config 错误）
 * 统一抛本异常，**不**让原生 `RdKafka\Exception` 直接逃逸到业务层。
 *
 * ## 设计动机
 *
 * - 业务层 `try/catch` 只需要 `catch (KafkaException $e)`，不用关心底层 librdkafka 报错码
 * - 内部可以随版本切换底层（v0.1 ext-rdkafka / v0.5+ 评估扩展）
 * - 与 Laravel `RuntimeException` 兼容，框架统一处理
 *
 * ## 子类（v0.1）
 *
 * - {@see SerializationException} 序列化失败
 * - {@see DlqException} 写 DLQ 失败
 *
 * 子类按"业务语义"细分，业务层可以按 `instanceof` 分别处理。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 用单一 `CouldNotPublishMessageException` + 一堆 `__construct($message, $code, $previous)`，
 * 我们拆出 SerializationException / DlqException 子类，业务层 try/catch 颗粒度更细。
 */
class KafkaException extends RuntimeException
{
}
