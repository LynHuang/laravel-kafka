<?php

declare(strict_types=1);

namespace LaravelKafka\Producer;

/**
 * 发送到 Kafka 的消息值对象（v0.1 不可变）。
 *
 * ## 角色
 *
 * 封装 Kafka 消息的"四要素"：
 *  - `payload`：消息体（业务方塞 PHP serialize 后的字符串 / JSON / 自定义）
 *  - `headers`：Kafka headers（key-value，业务方塞 trace / queue / connection / serializer 等元信息）
 *  - `key`：partition 路由键（同 key → 同 partition → 严格顺序）
 *  - `partition`：显式指定 partition（**生产侧不推荐**，破坏 broker 负载均衡）
 *  - `timestampMs`：消息时间戳（null = broker 当前时间）
 *
 * ## 不可变 + with* 模式
 *
 * 用 `withHeader()` / `withKey()` / `withHeaders()` 返回新实例，**不**修改原对象。
 * `KafkaQueue::buildMessage()` 是用 named arguments 构造，业务方扩展时也建议用 `with*`。
 *
 * ## v0.1 vs v0.2
 *
 * v0.1：`Message` 不含 `topic`（topic 由 `Producer::send($topic, $message)` 单独传）。
 * v0.2+ 评估：在 `Message` 上加 `?string $topic` 字段，`FakeMessageStorage` 解析时直接拿（避免二元组）。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 用 `RdKafka\ProducerTopic::produv()` + 数组传参，
 * 我们用值对象 + 类型签名，业务方 IDE 补全友好 + 重构安全。
 */
final class Message
{
    /**
     * 消息体（已序列化）。
     */
    private string $payload;

    /**
     * Kafka headers（key-value）。
     *
     * @var array<string,string>
     */
    private array $headers;

    /**
     * 路由键；同 key 永远落同分区。
     */
    private ?string $key;

    /**
     * 显式指定 partition（**生产侧不推荐**，broker 负载均衡失效）。
     */
    private ?int $partition;

    /**
     * 消息时间戳（ms）；null = broker 当前时间。
     */
    private ?int $timestampMs;

    /**
     * @param string                $payload    消息体（已序列化）
     * @param array<string,string>  $headers    Kafka headers
     * @param string|null           $key        路由键；同 key 永远落同分区
     * @param int|null              $partition  显式指定 partition（**生产侧不推荐**，broker 负载均衡失效）
     * @param int|null              $timestampMs 消息时间戳（ms）；null = broker 当前时间
     */
    public function __construct(
        string $payload,
        array $headers = [],
        ?string $key = null,
        ?int $partition = null,
        ?int $timestampMs = null
    ) {
        $this->payload = $payload;
        $this->headers = $headers;
        $this->key = $key;
        $this->partition = $partition;
        $this->timestampMs = $timestampMs;
    }

    /**
     * 消息体（已序列化）。
     *
     * @return string 字节串
     */
    public function payload(): string
    {
        return $this->payload;
    }

    /**
     * 全部 Kafka headers。
     *
     * @return array<string,string>
     */
    public function headers(): array
    {
        return $this->headers;
    }

    /**
     * 单个 header 查询。
     *
     * @param string $name header 名（业务方建议用 {@see \LaravelKafka\Support\Header} 常量）
     * @param string|null $default 不存在时返回的默认值
     * @return string|null header value / default
     */
    public function header(string $name, ?string $default = null): ?string
    {
        return $this->headers[$name] ?? $default;
    }

    /**
     * partition 路由键。
     *
     * 同 key 永远落同 partition（librdkafka `consistent_random` 分区器）→ 严格顺序保证。
     * null = librdkafka 轮询 partition（**不保证顺序**）。
     *
     * @return string|null
     */
    public function key(): ?string
    {
        return $this->key;
    }

    /**
     * 显式指定 partition。
     *
     * v0.1 默认 null（让 broker 选）。业务方**不应该**用这个字段（破坏负载均衡），
     * 想控制分区路由请用 `key`。
     *
     * @return int|null
     */
    public function partition(): ?int
    {
        return $this->partition;
    }

    /**
     * 消息时间戳（ms）。
     *
     * null = broker 当前时间。
     * 业务方可以用这个字段做"消息回放"（设历史时间戳）。
     *
     * @return int|null
     */
    public function timestampMs(): ?int
    {
        return $this->timestampMs;
    }

    /**
     * 批量加 header（不可变：返回新实例）。
     *
     * 新 headers 合并到原 headers 后面，**同 key 覆盖**。
     *
     * @param array<string,string> $headers 新增的 headers
     * @return self 新实例（原对象不变）
     */
    public function withHeaders(array $headers): self
    {
        return new self(
            $this->payload,
            array_merge($this->headers, $headers),
            $this->key,
            $this->partition,
            $this->timestampMs,
        );
    }

    /**
     * 单个 header（不可变）。
     *
     * @param string $name
     * @param string $value
     * @return self 新实例
     */
    public function withHeader(string $name, string $value): self
    {
        return $this->withHeaders([$name => $value]);
    }

    /**
     * 改 key（不可变）。
     *
     * @param string|null $key 新 key（null = 解除路由）
     * @return self 新实例
     */
    public function withKey(?string $key): self
    {
        return new self(
            $this->payload,
            $this->headers,
            $key,
            $this->partition,
            $this->timestampMs,
        );
    }
}
