<?php

declare(strict_types=1);

namespace LaravelKafka\Exceptions;

use RuntimeException;

/**
 * 序列化失败异常（v0.1 基础类）。
 *
 * ## 角色
 *
 * 由 {@see \LaravelKafka\Producer\Serializer\Serializer} 实现（PhpSerializer / JsonSerializer）
 * 在 encode/decode 失败时抛出，标识 payload **不可序列化**（不是临时网络错误）。
 *
 * ## 业务层处理
 *
 * - `HybridFailedJobHandler` 默认把 `SerializationException` 列入 `fatal_exceptions`（不重试，直接 DLQ）
 * - 业务层可在 `kafka.php` 配置 `failed.fatal_exceptions` 自定义
 *
 * ## 为什么不重试
 *
 * 序列化失败是 **payload 本身不可序列化**（如对象有循环引用、PHP serialize 不支持闭包、JSON 编码 NaN），
 * 重试 100% 失败。直接 DLQ + 报警，让业务方修复 payload 构造逻辑。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 把序列化错误混在 `KafkaException` 里，颗粒度粗；我们独立成类，方便配置 fatal 列表。
 */
class SerializationException extends RuntimeException
{
}
