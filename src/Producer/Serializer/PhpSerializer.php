<?php

declare(strict_types=1);

namespace LaravelKafka\Producer\Serializer;

use LaravelKafka\Exceptions\SerializationException;

/**
 * PHP serialize 序列化器（v0.1 默认）。
 *
 * ## 角色
 *
 * 用 PHP 原生 `serialize()` / `unserialize()` 编解码 payload。
 * **与 Laravel `Illuminate\Queue\Queue::createPayload` 行为一致**——
 * v0.1 默认序列化器。
 *
 * ## 优缺点
 *
 * - ✅ 优点：与原生 Laravel Job payload **二进制兼容**（消费端 unserialize 出来的就是 `Job` 对象）
 * - ❌ 缺点：跨语言消费需要先 unserialize（Node / Go 消费时要先用 PHP 反序列化）
 *
 * ## 安全
 *
 * `unserialize($raw, ['allowed_classes' => true])` **允许**任意类反序列化。
 * 业务方如果用了不可信 source 的 payload，建议切到 `JsonSerializer`。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 默认 `JsonSerializer`；我们默认 `PhpSerializer`（兼容 Laravel Job payload）。
 */
final class PhpSerializer implements Serializer
{
    /**
     * 编码任意 PHP 值为字节串。
     *
     * @param mixed $value 任意 PHP 值（标量 / 数组 / 对象）
     * @return string PHP serialize 后的字节串
     * @throws SerializationException PHP serialize 失败时（罕见，多数值都能 serialize）
     */
    public function encode($value): string
    {
        try {
            return serialize($value);
        } catch (\Throwable $e) {
            throw new SerializationException(
                sprintf('PhpSerializer encode failed: %s', $e->getMessage()),
                0,
                $e
            );
        }
    }

    /**
     * 解码字节串回 PHP 值。
     *
     * ## 边界
     *
     * - 空字符串 → 返回 `null`（业务方可能传空 payload）
     * - `unserialize` 返回 `false` + raw 不等于 `'b:0;'` → 抛异常（实际是失败，不是 PHP 序列化的 false）
     * - 异常（类不存在等） → 抛 `SerializationException`
     *
     * @param string $raw 字节串
     * @return mixed 解码后的值（对象 / 数组 / 标量）
     * @throws SerializationException 不可解码时
     */
    public function decode(string $raw)
    {
        if ($raw === '') {
            return null;
        }
        try {
            $value = unserialize($raw, ['allowed_classes' => true]);
        } catch (\Throwable $e) {
            throw new SerializationException(
                sprintf('PhpSerializer decode failed: %s', $e->getMessage()),
                0,
                $e
            );
        }
        if ($value === false && $raw !== 'b:0;') {
            throw new SerializationException('PhpSerializer decode returned false on non-empty payload.');
        }
        return $value;
    }

    /**
     * 序列化器唯一标识（写入 header `x-serializer`）。
     *
     * @return string 'php'
     */
    public function name(): string
    {
        return 'php';
    }
}
