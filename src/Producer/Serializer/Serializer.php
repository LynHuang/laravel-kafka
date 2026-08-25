<?php

declare(strict_types=1);

namespace LaravelKafka\Producer\Serializer;

use LaravelKafka\Exceptions\SerializationException;

/**
 * 消息体序列化器接口（v0.1）。
 *
 * ## 角色
 *
 * 定义 produce 端 encode / consume 端 decode 的统一契约。
 * 消费端根据 header 里的 `x-serializer` 字段（{@see \LaravelKafka\Support\Header::SERIALIZER}）
 * 选对应实现来 decode —— 避免 producer 用 PHP serialize、consumer 用 JSON decode 的不匹配。
 *
 * ## 实现
 *
 * - v0.1 默认 {@see PhpSerializer}（与 Laravel `Queue::createPayload` 一致）
 * - v0.2 新增 {@see JsonSerializer}（跨语言场景）
 * - v0.4 评估 Avro + Schema Registry（强 schema 治理）
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 默认 JsonSerializer + 把 serializer name 塞 message option；
 * 我们默认 PhpSerializer（兼容 Laravel Job payload）+ name 写入 header（消费端自取）。
 */
interface Serializer
{
    /**
     * 把任意 PHP 值编码成字符串。
     *
     * 失败必须抛 {@see SerializationException}（不抛 `RuntimeException`），
     * 这样 {@see \LaravelKafka\Queue\Failed\HybridFailedJobHandler} 的 fatal_exceptions 配置
     * 才能精确识别。
     *
     * @param mixed $value 待序列化的值（任意 PHP 类型）
     * @return string 序列化后的字节串
     * @throws SerializationException 不可序列化时
     */
    public function encode(mixed $value): string;

    /**
     * 把字节串解码回 PHP 值。
     *
     * 失败必须抛 `SerializationException`。
     * 业务方可以传入 raw 字节（来自 librdkafka message->payload），
     * 也可以传入空字符串 / 非合法 base64（按实现决定默认行为）。
     *
     * @param string $raw 待解码的字节串
     * @return mixed 解码后的值
     * @throws SerializationException 不可解码时
     */
    public function decode(string $raw);

    /**
     * 序列化器唯一标识。
     *
     * 写入 message header 的 `x-serializer` 字段，**消费端按这个 name 选反序列化器**。
     * 命名约定：小写、字母+数字+短横线（如 `php` / `json` / `avro`）。
     *
     * @return string 序列化器标识（写入 header 的字符串）
     */
    public function name(): string;
}
