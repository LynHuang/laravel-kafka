<?php

declare(strict_types=1);

namespace LaravelKafka\Replay;

use LaravelKafka\Config\KafkaConfig;
use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Producer\Message;
use LaravelKafka\Producer\ProducerFactory;

/**
 * Replay 实际 reproduce 执行器（v0.5.3 实现，v0.3 设计）。
 *
 * ## 用途
 *
 * 把源 topic 时间窗口内的消息重新 produce 到目标 topic。
 *
 * ## 流程
 *
 *  1. 用独立 consumer group（不影响主消费者）拿 topic metadata → partition 列表
 *  2. `offsetsForTimes` 找每个 partition 在 `from` 时间戳对应的 offset
 *  3. 每个 partition `assign(fromOffset)` + `consume` 遍历
 *  4. 消息 timestamp <= `to` 时间戳 → `Producer::send` 重放到 target-topic
 *  5. 超过 `to` → 结束该 partition
 *
 * ## 消息重放
 *
 * 原始 payload + headers（traceparent 等）+ key 原样重放，消费端无感知。
 *
 * ## 限制
 *
 * - 顺序：按 partition 顺序重放（同 partition 内保序）
 * - 幂等：重复执行会重复 produce（业务方自行用目标 topic 幂等处理）
 */
final class ReplayExecutor
{
    /**
     * @var ProducerFactory
     */
    private ProducerFactory $producerFactory;

    /**
     * @param ProducerFactory $producerFactory 生产者工厂（重放用）
     */
    public function __construct(ProducerFactory $producerFactory)
    {
        $this->producerFactory = $producerFactory;
    }

    /**
     * 执行重放。
     *
     * @param string $topic 源 topic
     * @param int $fromSec 起始时间戳（**秒**，TimeWindowParser 返回秒）
     * @param int $toSec 结束时间戳（**秒**）
     * @param string $targetTopic 目标 topic
     * @param string $group 独立 consumer group（不影响主消费者）
     * @param KafkaConfig $config Kafka 连接配置（brokers / producer 配置）
     * @return array{replayed: int, partitions: int, window: array{from: int, to: int}}
     * @throws KafkaException broker 不可达 / 找不到 topic / 重放失败
     */
    public function execute(
        string $topic,
        int $fromSec,
        int $toSec,
        string $targetTopic,
        string $group,
        KafkaConfig $config
    ): array {
        if ($fromSec >= $toSec) {
            throw new KafkaException('ReplayExecutor: from must be before to.');
        }
        // v0.5.3 fix: parseWindow 返回秒, 但 Kafka 时间戳是毫秒 (msg->timestamp 13 位).
        // 转 ms 后才能和 offsetsForTimes / timestamp 比较.
        $fromMs = $fromSec * 1000;
        $toMs = $toSec * 1000;
        $brokers = $config->brokers();
        if ($brokers === '') {
            throw new KafkaException('ReplayExecutor: brokers must not be empty.');
        }

        $consumer = $this->buildConsumer($brokers, $group);

        // 1. 拿 topic partition 列表
        $partitions = [];
        try {
            $meta = $consumer->getMetadata(true, null, 8000);
            foreach ($meta->getTopics() as $t) {
                if ($t->getTopic() !== $topic) {
                    continue;
                }
                foreach ($t->getPartitions() as $p) {
                    $partitions[] = $p->getId();
                }
            }
        } catch (\Throwable $e) {
            $consumer->close();
            throw new KafkaException(sprintf(
                'ReplayExecutor: failed to fetch metadata for topic "%s": %s',
                $topic,
                $e->getMessage()
            ));
        }

        if (count($partitions) === 0) {
            $consumer->close();
            throw new KafkaException(sprintf(
                'ReplayExecutor: topic "%s" not found or has no partitions.',
                $topic
            ));
        }

        // 2. offsetsForTimes 找每个 partition 的 from/to offset（TopicPartition.offset = 时间戳 ms）
        $fromParts = [];
        $toParts = [];
        foreach ($partitions as $partitionId) {
            $fromParts[] = new \RdKafka\TopicPartition($topic, $partitionId, $fromMs);
            $toParts[] = new \RdKafka\TopicPartition($topic, $partitionId, $toMs);
        }
        try {
            $resolvedFrom = $consumer->offsetsForTimes($fromParts, 8000);
            $resolvedTo = $consumer->offsetsForTimes($toParts, 8000);
        } catch (\Throwable $e) {
            $consumer->close();
            throw new KafkaException(sprintf(
                'ReplayExecutor: offsetsForTimes failed: %s',
                $e->getMessage()
            ));
        }

        // 建立 partition id → offset 映射
        $fromOffsetByPartition = [];
        $toOffsetByPartition = [];
        foreach ($resolvedFrom as $fp) {
            $fromOffsetByPartition[$fp->getPartition()] = $fp->getOffset();
        }
        foreach ($resolvedTo as $fp) {
            $toOffsetByPartition[$fp->getPartition()] = $fp->getOffset();
        }

        // 3. 每个 partition 从 from offset 遍历到 to offset（用 offset 上限, 避免死循环）
        $producer = $this->producerFactory->make($config);
        $replayed = 0;

        try {
            foreach ($partitions as $partitionId) {
                $startOffset = $fromOffsetByPartition[$partitionId] ?? -1;
                if ($startOffset < 0) {
                    continue; // 该 partition 在 from 时间点无消息
                }
                $endOffset = $toOffsetByPartition[$partitionId] ?? PHP_INT_MAX;
                if ($endOffset < 0) {
                    $endOffset = PHP_INT_MAX; // to 之后无消息 → 到末尾
                }

                $consumer->assign([new \RdKafka\TopicPartition($topic, $partitionId, $startOffset)]);

                $timeoutCount = 0;
                while (true) {
                    $msg = $consumer->consume(1000);
                    if ($msg === null || $msg->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
                        // v0.5.3: 连续 30 次超时 (约 30s 无消息) → 结束该 partition, 防死循环
                        if (++$timeoutCount > 30) {
                            break;
                        }
                        continue;
                    }
                    if ($msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                        break;
                    }
                    if ($msg->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                        break;
                    }
                    $timeoutCount = 0;

                    // 超出窗口: 用 offset 上限 (比 timestamp 更可靠, 不依赖 broker 时间戳)
                    if ($msg->offset >= $endOffset) {
                        break;
                    }

                    $this->produce($producer, $targetTopic, $msg);
                    $replayed++;
                }
            }
        } finally {
            $consumer->close();
            $producer->flush(5000);
        }

        return [
            'replayed' => $replayed,
            'partitions' => count($partitions),
            'window' => ['from' => $fromSec, 'to' => $toSec],
        ];
    }

    /**
     * 内部：构造独立 consumer（不影响主消费者的 group）。
     *
     * @param string $brokers
     * @param string $group
     * @return \RdKafka\KafkaConsumer
     */
    private function buildConsumer(string $brokers, string $group): \RdKafka\KafkaConsumer
    {
        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', $brokers);
        $conf->set('group.id', $group);
        $conf->set('enable.auto.commit', 'false');
        $conf->set('auto.offset.reset', 'earliest');
        $conf->set('socket.timeout.ms', '8000');
        $conf->set('client.id', 'laravel-kafka-replay-' . getmypid());

        return new \RdKafka\KafkaConsumer($conf);
    }

    /**
     * 内部：重放一条消息到目标 topic。
     *
     * @param \LaravelKafka\Producer\Producer $producer
     * @param string $targetTopic
     * @param \RdKafka\Message $msg
     * @return void
     */
    private function produce(\LaravelKafka\Producer\Producer $producer, string $targetTopic, \RdKafka\Message $msg): void
    {
        $headers = is_array($msg->headers) ? $msg->headers : [];
        // 强制 header 值为 string（librdkafka 可能返回 int 等）
        $normalized = [];
        foreach ($headers as $k => $v) {
            $normalized[(string) $k] = (string) $v;
        }

        $producer->send($targetTopic, new Message(
            (string) $msg->payload,
            $normalized,
            $msg->key !== null ? (string) $msg->key : null
        ));
    }
}
