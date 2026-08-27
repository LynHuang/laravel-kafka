<?php

declare(strict_types=1);

namespace LaravelKafka\Config;

use LaravelKafka\Exceptions\KafkaException;

/**
 * Kafka 连接配置值对象（v0.1 不可变）。
 *
 * ## 角色
 *
 * 封装 `config/kafka.php` 里一个 connection 的全部配置。
 * 所有 Kafka 连接层组件（Producer / Consumer / KafkaQueue / FailedHandler）都通过本对象访问配置，
 * 避免散弹式 `config('kafka.connections.default.xxx')` 调用。
 *
 * ## 不可变
 *
 * 构造时一次性写所有属性，**没有** setter。业务方想换配置必须重新 `fromArray()` / `new`。
 *
 * ## librdkafka 配置转换
 *
 * 提供 3 个方法把"业务方友好的配置"转成"librdkafka 接受的配置"：
 *  - `toRdKafkaConfig()`：通用部分（client.id / bootstrap.servers / SSL / SASL）
 *  - `toProducerRdKafkaConfig()`：上面 + producer 配置
 *  - `toConsumerRdKafkaConfig()`：通用 + group.id / enable.auto.commit=false + consumer 配置
 *
 * ## v0.1 协议支持
 *
 * - `PLAINTEXT`（默认）
 * - `SSL`（单向 TLS）
 * - `SASL_PLAINTEXT`（PLAINTEXT + SASL）
 * - `SASL_SSL`（SSL + SASL）
 *
 *
 * mateusjunges 没有集中配置类，散在 `Manager` 里 + 用 `config('kafka.*')` 散弹调用；
 * 我们用值对象 + 不可变 + 单一事实源（SoT）。
 */
final class KafkaConfig
{
    /**
     * Producer 字段名 → librdkafka key 翻译表。
     *
     * 业务方在 config/kafka.php 写业务方友好命名（`compression` / `batch_size` / `linger_ms`），
     * 翻译成 librdkafka 实际接受的 key（`compression.type` / `batch.size` / `linger.ms`）。
     *
     * 命名来源：librdkafka [CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
     *
     * @var array<string,string>
     */
    private const PRODUCER_KEY_MAP = [
        'compression'          => 'compression.type',
        'batch_size'           => 'batch.size',
        'linger_ms'            => 'linger.ms',
        'request_timeout_ms'   => 'request.timeout.ms',
        'message_timeout_ms'   => 'message.timeout.ms',
        'enable_idempotence'   => 'enable.idempotence',
        // 'acks' 已经是 librdkafka 原生名，无需翻译
    ];

    /**
     * Consumer 字段名 → librdkafka key 翻译表。
     *
     * `group_id` / `auto_offset_reset` / `isolation_level` 已在
     * {@see toConsumerRdKafkaConfig()} 顶部显式翻译，这里只列剩余的。
     *
     * @var array<string,string>
     */
    private const CONSUMER_KEY_MAP = [
        'max_poll_interval_ms'  => 'max.poll.interval.ms',
        'session_timeout_ms'    => 'session.timeout.ms',
        'heartbeat_interval_ms' => 'heartbeat.interval.ms',
        'fetch_min_bytes'       => 'fetch.min.bytes',
        'fetch_max_bytes'       => 'fetch.max.bytes',
        'enable_auto_commit'    => 'enable.auto.commit',
    ];
    /**
     * 连接名（如 "default" / "reports"）。
     */
    private string $name;

    /**
     * 引导 broker 列表（逗号分隔，如 "host1:9092,host2:9092"）。
     */
    private string $brokers;

    /**
     * 客户端标识（librdkafka `client.id`）。
     */
    private string $clientId;

    /**
     * 安全协议（PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL）。
     */
    private string $protocol;

    /**
     * SASL 配置（mechanism / username / password）。
     *
     * @var array<string,string>
     */
    private array $sasl;

    /**
     * SSL 配置（ca_location / cert_location / key_location）。
     *
     * @var array<string,string>
     */
    private array $ssl;

    /**
     * 默认 topic。
     */
    private string $defaultTopic;

    /**
     * 队列名 → topic 映射。
     *
     * @var array<string,string>
     */
    private array $topics;

    /**
     * 生产者配置（librdkafka 透传）。
     *
     * @var array<string,mixed>
     */
    private array $producer;

    /**
     * 消费者配置。
     *
     * @var array<string,mixed>
     */
    private array $consumer;

    /**
     * 失败处理配置（driver / database / dlq / hybrid）。
     *
     * @var array<string,mixed>
     */
    private array $failed;

    /**
     * 延迟消息配置（v0.3 时间轮预留）。
     *
     * @var array<string,mixed>
     */
    private array $delay;

    /**
     * 回溯配置（v0.3 replay 预留）。
     *
     * @var array<string,mixed>
     */
    private array $replay;

    /**
     * 默认序列化器标识（'php' / 'json'，v0.5.0 配置化）。
     *
     * - push 侧：`buildMessage` 的 `x-serializer` header 用此值（替代硬编码 'php'）
     * - consume 侧：裸事件（非 Laravel Job）无 `x-serializer` header 时用此值
     *
     * 业务方在 `config/kafka.php` 配 `serializer => env('KAFKA_SERIALIZER', 'php')`。
     */
    private string $serializer;

    /**
     * 从业务方 array 配置构造（ServiceProvider boot 时调）。
     *
     * 字段映射：
     *  - `brokers` → brokers
     *  - `client_id` → clientId（默认 `laravel-kafka`）
     *  - `protocol` → protocol（默认 `PLAINTEXT`）
     *  - `queue` → defaultTopic（默认 `laravel-jobs`）—— 兼容 v0.1 早期命名
     *  - `topics` / `producer` / `consumer` / `failed` / `delay` / `replay` → 各自
     *
     * @param string $name connection 名
     * @param array<string,mixed> $config 业务方配置
     * @return self
     * @throws KafkaException 校验失败
     */
    public static function fromArray(string $name, array $config): self
    {
        return new self(
            $name,
            (string) ($config['brokers'] ?? ''),
            (string) ($config['client_id'] ?? 'laravel-kafka'),
            (string) ($config['protocol'] ?? 'PLAINTEXT'),
            (array) ($config['sasl'] ?? []),
            (array) ($config['ssl'] ?? []),
            isset($config['queue']) ? (string) $config['queue'] : '',
            (array) ($config['topics'] ?? []),
            (array) ($config['producer'] ?? []),
            (array) ($config['consumer'] ?? []),
            (array) ($config['failed'] ?? []),
            (array) ($config['delay'] ?? []),
            (array) ($config['replay'] ?? []),
            (string) ($config['serializer'] ?? 'php'),
        );
    }

    /**
     * @param string                $name             连接名（如 "default" / "reports"）
     * @param string                $brokers          引导 broker 列表（逗号分隔，如 "host1:9092,host2:9092"）
     * @param string                $clientId         客户端标识（librdkafka `client.id`）
     * @param string                $protocol         PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
     * @param array<string,string>  $sasl             SASL 配置（mechanism / username / password）
     * @param array<string,string>  $ssl              SSL 配置（ca_location / cert_location / key_location）
     * @param string                $defaultTopic     默认 topic
     * @param array<string,string>  $topics           队列名 → topic 映射
     * @param array<string,mixed>   $producer         生产者配置（librdkafka 透传）
     * @param array<string,mixed>   $consumer         消费者配置
     * @param array<string,mixed>   $failed           失败处理配置（driver / database / dlq / hybrid）
     * @param array<string,mixed>   $delay            延迟消息配置（v0.3 时间轮预留）
     * @param array<string,mixed>   $replay           回溯配置（v0.3 replay 预留）
     * @param string                $serializer       默认序列化器（'php' / 'json'，v0.5.0 配置化）
     * @throws KafkaException protocol 非法 / brokers 空 / defaultTopic 空
     */
    public function __construct(
        string $name,
        string $brokers,
        string $clientId,
        string $protocol,
        array $sasl,
        array $ssl,
        string $defaultTopic,
        array $topics,
        array $producer,
        array $consumer,
        array $failed,
        array $delay,
        array $replay,
        string $serializer = 'php'
    ) {
        $this->name = $name;
        $this->brokers = $brokers;
        $this->clientId = $clientId;
        $this->protocol = $protocol;
        $this->sasl = $sasl;
        $this->ssl = $ssl;
        $this->defaultTopic = $defaultTopic;
        $this->topics = $topics;
        $this->producer = $producer;
        $this->consumer = $consumer;
        $this->failed = $failed;
        $this->delay = $delay;
        $this->replay = $replay;
        $this->serializer = $serializer;

        $this->validateProtocol($protocol);
        if ($brokers === '') {
            throw new KafkaException('Kafka brokers must not be empty.');
        }
        if ($defaultTopic === '') {
            throw new KafkaException('Kafka default topic must not be empty.');
        }
    }

    /**
     * 连接名。
     *
     * @return string
     */
    public function name(): string
    {
        return $this->name;
    }

    /**
     * 引导 broker 列表（逗号分隔）。
     *
     * @return string
     */
    public function brokers(): string
    {
        return $this->brokers;
    }

    /**
     * 客户端标识（librdkafka `client.id`）。
     *
     * @return string
     */
    public function clientId(): string
    {
        return $this->clientId;
    }

    /**
     * 安全协议。
     *
     * @return string PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
     */
    public function protocol(): string
    {
        return $this->protocol;
    }

    /**
     * SASL 配置。
     *
     * @return array<string,string>
     */
    public function sasl(): array
    {
        return $this->sasl;
    }

    /**
     * SSL 配置。
     *
     * @return array<string,string>
     */
    public function ssl(): array
    {
        return $this->ssl;
    }

    /**
     * 默认 topic（兜底）。
     *
     * @return string
     */
    public function defaultTopic(): string
    {
        return $this->defaultTopic;
    }

    /**
     * 队列名 → topic 映射表。
     *
     * @return array<string,string>
     */
    public function topics(): array
    {
        return $this->topics;
    }

    /**
     * 解析 Laravel 逻辑队列名 → 物理 Kafka topic。
     *
     * ## 优先级
     *
     *  1. `$this->topics[$queue]` 显式映射（业务方长期配置）
     *  2. `$queue` 自身（同名）
     *  3. `$this->defaultTopic` 兜底
     *
     * v0.2 `KafkaQueue::resolveTopicWithOptions()` 增加 `options['topic']` 优先级 0，覆盖本方法。
     *
     * @param string|null $queue Laravel 逻辑队列名
     * @return string 物理 topic 名
     */
    public function resolveTopic(?string $queue): string
    {
        if ($queue !== null && isset($this->topics[$queue])) {
            return (string) $this->topics[$queue];
        }

        if ($queue !== null && $queue !== '') {
            return $queue;
        }

        return $this->defaultTopic;
    }

    /**
     * 生产者配置（librdkafka 透传）。
     *
     * @return array<string,mixed>
     */
    public function producer(): array
    {
        return $this->producer;
    }

    /**
     * 消费者配置（librdkafka 透传）。
     *
     * @return array<string,mixed>
     */
    public function consumer(): array
    {
        return $this->consumer;
    }

    /**
     * 失败处理配置。
     *
     * @return array<string,mixed>
     */
    public function failed(): array
    {
        return $this->failed;
    }

    /**
     * 延迟消息配置（v0.3 时间轮预留）。
     *
     * @return array<string,mixed>
     */
    public function delay(): array
    {
        return $this->delay;
    }

    /**
     * 回溯配置（v0.3 replay 预留）。
     *
     * @return array<string,mixed>
     */
    public function replay(): array
    {
        return $this->replay;
    }

    /**
     * 默认序列化器标识（'php' / 'json'，v0.5.0 配置化）。
     *
     * - push 侧 `KafkaQueue::buildMessage` 的 `x-serializer` header 默认值
     * - consume 侧 `NativeHandler` 裸事件无 header 时的默认
     *
     * @return string 'php' / 'json' / 自定义（业务方注册的）
     */
    public function serializer(): string
    {
        return $this->serializer;
    }

    /**
     * 通用 librdkafka 配置（client.id / bootstrap.servers + SSL/SASL）。
     *
     * @return array<string,string>
     */
    public function toRdKafkaConfig(): array
    {
        $conf = [
            'client.id' => $this->clientId,
            'bootstrap.servers' => $this->brokers,
        ];

        if ($this->protocol === 'SSL' || $this->protocol === 'SASL_SSL') {
            foreach ($this->ssl as $k => $v) {
                if ($v !== null && $v !== '') {
                    $conf['ssl.' . $this->sslKeyMap((string) $k)] = (string) $v;
                }
            }
        }

        if ($this->protocol === 'SASL_PLAINTEXT' || $this->protocol === 'SASL_SSL') {
            $mechanism = $this->sasl['mechanism'] ?? null;
            $username = $this->sasl['username'] ?? null;
            $password = $this->sasl['password'] ?? null;
            if ($mechanism !== null && $mechanism !== '') {
                $conf['sasl.mechanism'] = (string) $mechanism;
            }
            if ($username !== null && $username !== '') {
                $conf['sasl.username'] = (string) $username;
            }
            if ($password !== null && $password !== '') {
                $conf['sasl.password'] = (string) $password;
            }
            $conf['security.protocol'] = $this->protocol;
        } elseif ($this->protocol === 'SSL') {
            $conf['security.protocol'] = 'SSL';
        }

        return $conf;
    }

    /**
     * 完整 producer 配置（通用 + producer 子配置，已翻译 key）。
     *
     * @return array<string,string>
     */
    public function toProducerRdKafkaConfig(): array
    {
        return array_merge(
            $this->toRdKafkaConfig(),
            $this->stringifyConfig($this->translateKeys($this->producer, self::PRODUCER_KEY_MAP))
        );
    }

    /**
     * 完整 consumer 配置（通用 + group.id / auto.commit=false + consumer 子配置，已翻译 key）。
     *
     * 关键：
     *  - `enable.auto.commit=false`（手动 commit，HandlerResult::ack 才提交）
     *  - `group.id` 默认 `laravel-default`
     *  - `auto.offset.reset` 默认 `error`（不自动跳到 latest，业务方必须显式管理 offset）
     *
     * @return array<string,string>
     */
    public function toConsumerRdKafkaConfig(): array
    {
        $conf = $this->toRdKafkaConfig();
        $conf['group.id'] = (string) ($this->consumer['group_id'] ?? 'laravel-default');
        $conf['enable.auto.commit'] = 'false';
        $conf['auto.offset.reset'] = (string) ($this->consumer['auto_offset_reset'] ?? 'error');
        if (isset($this->consumer['isolation_level'])) {
            $conf['isolation.level'] = (string) $this->consumer['isolation_level'];
        }

        // 排除已翻译的 key（group_id / auto_offset_reset / isolation_level）
        $excludedKeys = ['group_id', 'auto_offset_reset', 'isolation_level'];
        $remaining = array_diff_key($this->consumer, array_flip($excludedKeys));

        return array_merge(
            $conf,
            $this->stringifyConfig($this->translateKeys($remaining, self::CONSUMER_KEY_MAP))
        );
    }

    /**
     * 按 key 翻译表重命名数组 key（v0.4.1 hotfix: 之前 stringifyConfig 直接透传业务方友好 key 给 librdkafka，
     * 触发 "No such configuration property" 错误）。
     *
     * - 在映射表里的 key → 重命名为 librdkafka 原生名
     * - 不在映射表里的 key → 保留原名（业务方写了 librdkafka 原生名时直接通过）
     * - 翻译后**不**做 exclude 过滤（不重复逻辑），由调用方负责 exclude
     *
     * @param array<string,mixed> $src
     * @param array<string,string> $keyMap
     * @return array<string,mixed>
     */
    private function translateKeys(array $src, array $keyMap): array
    {
        $out = [];
        foreach ($src as $k => $v) {
            $newKey = $keyMap[(string) $k] ?? (string) $k;
            $out[$newKey] = $v;
        }
        return $out;
    }

    /**
     * 把业务方数组配置转成 librdkafka 接受的 `array<string,string>`。
     *
     * - 跳过 null 值（librdkafka 收到 null 会 warning）
     * - bool 转 'true' / 'false' 字符串
     * - 跳过 array / object 值（librdkafka 不接受嵌套）
     *
     * @param array<string,mixed> $src
     * @return array<string,string>
     */
    private function stringifyConfig(array $src): array
    {
        $out = [];
        foreach ($src as $k => $v) {
            if ($v === null) {
                continue;
            }
            if (is_bool($v)) {
                $out[(string) $k] = $v ? 'true' : 'false';
            } elseif (is_array($v) || is_object($v)) {
                continue;
            } else {
                $out[(string) $k] = (string) $v;
            }
        }
        return $out;
    }

    /**
     * 校验 protocol 字段。
     *
     * @param string $protocol
     * @throws KafkaException 非法 protocol
     */
    private function validateProtocol(string $protocol): void
    {
        $allowed = ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'];
        if (! in_array($protocol, $allowed, true)) {
            throw new KafkaException(sprintf(
                'Invalid Kafka protocol "%s". Allowed: %s',
                $protocol,
                implode(', ', $allowed)
            ));
        }
    }

    /**
     * SSL 字段短名 → librdkafka 字段名映射。
     *
     * 业务方写 `ca_location`，转成 librdkafka 的 `ssl.ca.location`。
     * 不在 map 里的 key 保持原样。
     *
     * @param string $short 业务方字段名
     * @return string librdkafka 字段名（不含 `ssl.` 前缀）
     */
    private function sslKeyMap(string $short): string
    {
        $map = [
            'ca_location' => 'ca.location',
            'cert_location' => 'cert.location',
            'key_location' => 'key.location',
            'key_password' => 'key.password',
        ];
        return $map[$short] ?? $short;
    }
}
