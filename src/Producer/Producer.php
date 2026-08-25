<?php

declare(strict_types=1);

namespace LaravelKafka\Producer;

use LaravelKafka\Exceptions\KafkaException;
use RdKafka\Conf;
use RdKafka\Producer as RdKafkaProducer;

/**
 * Kafka 生产者封装（v0.1 核心类）。
 *
 * ## 角色
 *
 * - 持有 `RdKafka\Producer` 实例
 * - 把 {@see Message} 翻译成 librdkafka `producev()` 调用
 * - 同步等待 delivery report（acks=all 场景下确保消息真的落到 broker）
 * - 抛 {@see KafkaException} 给上层捕获
 *
 * ## 同步 produce + 轮询
 *
 * 用 **同步** produce + 短轮询 (`poll(50)`) 等待 delivery report，
 * 避免外部调用方还要追踪"消息是否落地"。
 *
 * 配置 `enable.idempotence=true` + `acks=all` 时，delivery report 失败 = 消息**未**成功写盘，
 * 必须抛异常让调用方决定（重试 / DLQ / 报警）。
 *
 * ## 调用方契约
 *
 * - `kafka:work` 长驻进程退出前**必须**调 `flush($timeoutMs)`，否则 in-flight 消息丢失
 * - 业务方不直接 `new Producer`（走 {@see ProducerFactory::make()} 单例）
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 用异步 produce + 业务方手动调 `flush`，
 * 我们强制同步等待 → 业务方在 `KafkaQueue::pushRaw` 拿到 `KafkaException` 就能立刻感知失败。
 */
final class Producer
{
    /**
     * 底层 librdkafka producer。
     */
    private RdKafkaProducer $kafka;

    /**
     * 当前 in-flight 消息的 delivery 上下文（token → ['topic' => ..., 'key' => ...]）。
     *
     * `handleDeliveryReport()` 回调里通过 `$msg->opaque`（= token 字符串）查回来打错误日志。
     *
     * @var array<int, array{topic: string, key: ?string}>
     */
    private array $deliveryCallbacks = [];

    /**
     * 最近一次 produce 的 delivery report 是否成功。
     *
     * 由 `handleDeliveryReport()` 回调写入；`send()` 循环读它决定抛异常。
     *
     * @var bool
     */
    private $lastDeliverySucceeded = false;

    /**
     * @param RdKafkaProducer $kafka 底层 librdkafka 实例（由 {@see ProducerFactory::build()} 构造）
     */
    public function __construct(RdKafkaProducer $kafka)
    {
        $this->kafka = $kafka;
    }

    /**
     * 发送单条消息到指定 topic（同步等待 delivery report）。
     *
     * ## 流程
     *
     *  1. 取 `Message` 的 partition / key / headers / timestamp
     *  2. `producev()` 投递（msg_opaque = token）
     *  3. 循环 `poll(50)` 等待 `lastDeliverySucceeded = true`，最长 5000ms
     *  4. 失败抛 {@see KafkaException}（含 topic / key / 超时时间）
     *
     * @param string $topic 物理 topic 名（已解析过）
     * @param Message $message 消息值对象
     * @return int partition 编号（UDA 模式 = `RD_KAFKA_PARTITION_UA` 常量 = -1；显式指定时 = 实际编号）
     * @throws KafkaException produce 失败或 delivery report 标记错误时
     */
    public function send(string $topic, Message $message): int
    {
        $partition = $message->partition() ?? RD_KAFKA_PARTITION_UA;
        $key = $message->key();
        $headers = $this->normalizeHeaders($message->headers());

        $this->lastDeliverySucceeded = false;
        $token = random_int(0, PHP_INT_MAX);

        $this->deliveryCallbacks[$token] = [
            'topic' => $topic,
            'key' => $key,
        ];

        // 绑定当前消息的 delivery 回调（librdkafka 用 msg_opaque 传 token）
        // 注意：producev 在 RdKafka\ProducerTopic 上，不在 RdKafka\Producer 上
        $producerTopic = $this->kafka->newTopic($topic);
        $producerTopic->producev(
            $partition,
            0,
            $message->payload(),
            (string) $key,
            $headers,
            $message->timestampMs() ?? 0,
            (string) $token,
        );

        // 等待 delivery report
        $start = microtime(true);
        $timeoutMs = 5000;
        while (! $this->lastDeliverySucceeded && (microtime(true) - $start) * 1000 < $timeoutMs) {
            $this->kafka->poll(50);
        }

        unset($this->deliveryCallbacks[$token]);

        if (! $this->lastDeliverySucceeded) {
            throw new KafkaException(sprintf(
                'Kafka produce timeout after %d ms (topic=%s, key=%s).',
                $timeoutMs,
                $topic,
                $key ?? '(null)'
            ));
        }

        return $partition;
    }

    /**
     * 强制 flush 等待所有 in-flight 消息投递完成。
     *
     * ## 调用时机
     *
     * - `kafka:work` 退出前（信号处理 / 正常 shutdown）
     * - 单元测试 tearDown
     *
     * ## 失败处理
     *
     * `flush()` 返回非 0 = 有 in-flight 消息没投递完（broker 慢 / 队列满 / 网络抖动）。
     * 抛 {@see KafkaException} 让调用方决定（报警 / 重试 / 接受丢消息）。
     *
     * @param int $timeoutMs 最大等待时间（默认 10000ms = 10s）
     * @return void
     * @throws KafkaException flush 超时时
     */
    public function flush(int $timeoutMs = 10000): void
    {
        $code = $this->kafka->flush($timeoutMs);
        if ($code !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw new KafkaException(sprintf('Kafka flush failed with code %d.', $code));
        }
    }

    /**
     * 暴露底层 RdKafka\Producer（业务方需要直接调用 librdkafka API 时用）。
     *
     * 用法举例：业务方想用 librdkafka 的 `flush()` 但**不**走我们封装的同步 send。
     *
     * @return RdKafkaProducer
     */
    public function kafka(): RdKafkaProducer
    {
        return $this->kafka;
    }

    /**
     * 由 {@see ProducerFactory::build()} 在设置 `setDrMsgCb` 时绑定的回调。
     *
     * 业务方**不应该**直接调本方法（librdkafka 自己触发）。
     *
     * @internal
     * @param \RdKafka\Message $msg librdkafka delivery report
     * @return void
     */
    public function handleDeliveryReport(\RdKafka\Message $msg): void
    {
        $this->lastDeliverySucceeded = ($msg->err === RD_KAFKA_RESP_ERR_NO_ERROR);
        if (! $this->lastDeliverySucceeded) {
            $token = (int) $msg->opaque;
            $context = $this->deliveryCallbacks[$token] ?? ['topic' => '?', 'key' => null];
            // 记录到错误日志，抛异常由 send() 的循环触发
            error_log(sprintf(
                '[laravel-kafka] produce failed: topic=%s key=%s err=%d %s',
                $context['topic'],
                $context['key'] ?? '(null)',
                $msg->err,
                rd_kafka_err2str($msg->err)
            ));
        }
    }

    /**
     * 把业务方 headers 标准化成 librdkafka 接受的 `array<string,string>`。
     *
     * 过滤掉 null 值（librdkafka 收到 null 会 warning），强转 string。
     *
     * @param array<string,string> $headers 业务方原始 headers
     * @return array<string,string> librdkafka 接受的 headers
     */
    private function normalizeHeaders(array $headers): array
    {
        $out = [];
        foreach ($headers as $k => $v) {
            if ($v === null) {
                continue;
            }
            $out[(string) $k] = (string) $v;
        }
        return $out;
    }

    /**
     * 用 `Conf` 构造 Producer（{@see ProducerFactory} / 测试用）。
     *
     * 同时绑定 `setDrMsgCb` 回调到 `$this->handleDeliveryReport`。
     *
     * @param Conf $conf librdkafka 配置
     * @return self
     */
    public static function fromConf(Conf $conf): self
    {
        $instance = new self(new RdKafkaProducer($conf));
        $conf->setDrMsgCb(function ($kafka, $message) use ($instance) {
            $instance->handleDeliveryReport($message);
        });
        return $instance;
    }
}
