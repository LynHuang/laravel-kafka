<?php

declare(strict_types=1);

namespace LaravelKafka\Producer\Serializer;

use LaravelKafka\Exceptions\SerializationException;

/**
 * JSON 序列化器（v0.2 启用）。
 *
 * ## 角色
 *
 * 用 `json_encode()` / `json_decode()` 编解码 payload。
 * 适用**非 PHP 消费者**场景：Node / Go / Python 消费同一份 Laravel Job 时。
 *
 * ## 限制
 *
 * - 只支持 JSON-safe 数据类型（标量、数组、null）—— **不**支持对象
 * - 业务方在 push 时必须自己把对象 `->toArray()` / `->jsonSerialize()`
 *
 * ## 默认 flags
     *
 * - `JSON_UNESCAPED_UNICODE`：保留中文不转 `\uXXXX`
 * - `JSON_UNESCAPED_SLASHES`：URL 不转 `\/`
 *
 * ## 深度 512
 *
 * 业务方传深度嵌套 array 时不轻易爆栈（PHP 默认 512 已经很安全）。
 */
final class JsonSerializer implements Serializer
{
    /**
     * `json_encode` 的最大嵌套深度。
     */
    private int $depth;

    /**
     * `json_encode` 的 flag 组合。
     */
    private int $flags;

    public function __construct()
    {
        $this->depth = 512;
        $this->flags = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES;
    }

    /**
     * 编码 JSON-safe 数据为字符串。
     *
     * 业务方传**对象**会失败（`json_encode` 对没有 `JsonSerializable` 的对象返回 false），
     * 抛 `SerializationException` 让 {@see \LaravelKafka\Queue\Failed\HybridFailedJobHandler} 进 DLQ。
     *
     * @param mixed $value JSON-safe 数据（标量 / 数组 / null）
     * @return string JSON 字符串
     * @throws SerializationException 不可编码时
     */
    public function encode($value): string
    {
        try {
            $json = json_encode($value, $this->flags, $this->depth);
        } catch (\Throwable $e) {
            throw new SerializationException(
                sprintf('JsonSerializer encode failed: %s', $e->getMessage()),
                0,
                $e
            );
        }
        if ($json === false) {
            throw new SerializationException(
                sprintf('JsonSerializer encode failed: %s', json_last_error_msg())
            );
        }
        return $json;
    }

    /**
     * 解码 JSON 字符串为 PHP 值。
     *
     * ## 边界
     *
     * - 空字符串 → `null`（与 PhpSerializer 保持一致）
     * - `null` + raw = `'null'` → 合法 `null`，返回 `null`
     * - `null` + 其他 raw → 失败，抛 `SerializationException`
     *
     * ## 注意
     *
     * 第二个参数 `true` = 关联数组（不是 stdClass 对象），方便业务方直接 `$payload['job']` 取值。
     *
     * @param string $raw JSON 字符串
     * @return mixed 解码后的值
     * @throws SerializationException 不可解码时
     */
    public function decode(string $raw)
    {
        if ($raw === '') {
            return null;
        }
        try {
            $value = json_decode($raw, true, $this->depth, $this->flags);
        } catch (\Throwable $e) {
            throw new SerializationException(
                sprintf('JsonSerializer decode failed: %s', $e->getMessage()),
                0,
                $e
            );
        }
        if ($value === null && $raw !== 'null') {
            throw new SerializationException(
                sprintf('JsonSerializer decode failed: %s', json_last_error_msg())
            );
        }
        return $value;
    }

    /**
     * 序列化器唯一标识。
     *
     * @return string 'json'
     */
    public function name(): string
    {
        return 'json';
    }
}
