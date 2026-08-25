<?php

declare(strict_types=1);

namespace LaravelKafka\Consumer;

use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Producer\Message;
use LaravelKafka\Support\Header;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Message as RdKafkaMessage;

/**
 * Kafka 消费者封装。
 *
 * ## 职责
 *
 * - 持有 `RdKafka\KafkaConsumer` 实例
 * - 提供 `poll` / `ack` / `commitAsync` / `close` 接口
 * - 暴露当前 `Subscription`（topic 列表）
 *
 * ## 生命周期
 *
 * Consumer 与 Worker 是 **1:1 关系**：
 * - `WorkCommand` 持有一个 Consumer
 * - Consumer 又被 `KafkaJob` 引用（用于 ack）
 * - 不存在"一个 Consumer 多个 Worker"——partition 互斥由 Consumer Group 协议保证
 *
 * ## 三态返回值
 *
 * - 拉到消息 → 返回 `Message`
 * - 超时 / partition EOF → 返回 `null`（让 Worker 继续循环）
 * - librdkafka 内部错误 → 抛 `KafkaException`
 */
final class Consumer
{
    /**
     * librdkafka 消费者实例。
     *
     * @var RdKafkaConsumer
     */
    private RdKafkaConsumer $kafka;

    /**
     * 订阅描述（topic 列表）。
     *
     * @var Subscription
     */
    private Subscription $subscription;

    /**
     * 构造时立即订阅 topic。
     *
     * @param RdKafkaConsumer $kafka       librdkafka 实例
     * @param Subscription $subscription  订阅描述
     */
    public function __construct(RdKafkaConsumer $kafka, Subscription $subscription)
    {
        $this->kafka = $kafka;
        $this->subscription = $subscription;

        $this->kafka->subscribe($subscription->topics());
    }

    /**
     * 拉取一条消息。
     *
     * ## 三态
     *
     *  1. `RD_KAFKA_RESP_ERR_NO_ERROR` → 返回包装好的 `Message`
     *  2. `RD_KAFKA_RESP_ERR__PARTITION_EOF` / `RD_KAFKA_RESP_ERR__TIMED_OUT` → 返回 `null`（让 Worker 继续循环）
     *  3. 其他错误 → 抛 `KafkaException`
     *
     * @param int $timeoutMs 拉取超时（ms），默认 1000
     * @return Message|null 消息对象或 null（超时）
     * @throws KafkaException librdkafka 内部错误时
     */
    public function poll(int $timeoutMs = 1000): ?Message
    {
        $rdMsg = $this->kafka->consume($timeoutMs);

        switch ($rdMsg->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return $this->wrap($rdMsg);

            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                return null;

            default:
                throw new KafkaException(sprintf(
                    'Kafka consume error: code=%d %s',
                    $rdMsg->err,
                    rd_kafka_err2str($rdMsg->err)
                ));
        }
    }

    /**
     * 批量拉取消息（v0.3 引入）。
     *
     * ## 行为
     *
     * 循环调 `kafka->consume()` 拉消息直到满足以下任一条件：
     *  1. 已拉到 `max` 条消息
     *  2. 总耗时超过 `timeoutMs`（deadline-based）
     *  3. 拉到至少 1 条消息后，遇到 TIMED_OUT（早返回，不空等）
     *
     * ## 与 `poll` 的差异
     *
     * | 场景 | `poll` | `pollBatch` |
     * | --- | --- | --- |
     * | 拉 1 条 | 1 次 consume | 1 次 consume |
     * | 拉 N 条 | N 次 `poll`（每条单独 1s 超时） | 1 次 `pollBatch`（总超时 N 共享） |
     * | 性能 | 单条 commit 开销大 | 整批 commit 一次 |
     *
     * ## 业务方使用
     *
     * ```php
     * $messages = $consumer->pollBatch(50, 2000);  // 最多 50 条 / 最多等 2s
     * foreach ($messages as $message) {
     *     $result = $handler->handle($message);
     *     // 单条结果处理（ack/requeue/dlq）
     * }
     * $consumer->commitBatch();  // 整批 commit
     * ```
     *
     * ## 错误处理
     *
     * - 任何 librdkafka 内部错误（除 PARTITION_EOF / TIMED_OUT）→ 抛 KafkaException
     * - 业务方 catch 后**不**调 commitBatch，让消息下次重投
     *
     * @param int $max 最多拉取消息数（> 0）
     * @param int $timeoutMs 总超时（ms），默认 1000
     * @return array<int, Message> 0~max 条消息（空数组 = 超时无消息）
     * @throws KafkaException librdkafka 内部错误时
     * @throws \InvalidArgumentException $max <= 0 时
     */
    public function pollBatch(int $max, int $timeoutMs = 1000): array
    {
        if ($max <= 0) {
            throw new \InvalidArgumentException(sprintf(
                'pollBatch max must be > 0, got %d',
                $max
            ));
        }

        $messages = [];
        $deadline = microtime(true) + ($timeoutMs / 1000);
        $consecutiveTimeouts = 0;

        // v0.3 死循环防护：
        //  1. 达到 max → 退出
        //  2. deadline 到 → 退出
        //  3. 连续 2 次 TIMED_OUT（broker 立即说无消息）→ 退出
        //  4. 拉够 maxIters 次（极端情况防御）→ 退出
        $maxIters = $max + 2;

        for ($i = 0; $i < $maxIters; $i++) {
            if (count($messages) >= $max) {
                break;
            }
            if (microtime(true) >= $deadline) {
                break;
            }

            $remaining = (int) max(0, ($deadline - microtime(true)) * 1000);
            $perPollMs = $remaining > 0 ? $remaining : 1;
            $rdMsg = $this->kafka->consume($perPollMs);

            switch ($rdMsg->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $messages[] = $this->wrap($rdMsg);
                    $consecutiveTimeouts = 0;
                    break;

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    // 拉到至少 1 条 + 超时 → 早返回
                    if (count($messages) > 0) {
                        return $messages;
                    }
                    // 没消息：连续 2 次空 poll → 退出（避免 broker 立即返回 TIMED_OUT 时死循环）
                    $consecutiveTimeouts++;
                    if ($consecutiveTimeouts >= 2) {
                        return $messages;
                    }
                    break;

                default:
                    throw new KafkaException(sprintf(
                        'Kafka consume error: code=%d %s',
                        $rdMsg->err,
                        rd_kafka_err2str($rdMsg->err)
                    ));
            }
        }

        return $messages;
    }

    /**
     * 提交指定消息的 offset（同步 commit）。
     *
     * 业务方**不直接调**——`KafkaJob::delete()` 调本方法。
     *
     * @param RdKafkaMessage $rdMessage librdkafka 原始消息
     * @return void
     */
    public function ack(RdKafkaMessage $rdMessage): void
    {
        $this->kafka->commit($rdMessage);
    }

    /**
     * 提交当前所有 in-flight offset（异步 commit，性能更好）。
     *
     * 与 `ack()` 的区别：批量 commit，不等服务端 ack。
     * 适用场景：业务处理**成功**后批量 commit，提升吞吐。
     *
     * @return void
     */
    public function commitAsync(): void
    {
        $this->kafka->commitAsync();
    }

    /**
     * 整批 commit（v0.3 引入，配合 `pollBatch` 使用）。
     *
     * ## 行为
     *
     * 调用 `commitAsync()` commit 所有已消费但未 commit 的 offset。
     * librdkafka 没有"按消息列表 commit"的 API——它的 commit 是"消费位置"，
     * 调一次就 commit 当前位置及之前所有未提交的 offset。
     *
     * ## 业务方使用
     *
     * ```php
     * $messages = $consumer->pollBatch(50, 2000);
     * try {
     *     foreach ($messages as $message) {
     *         $handler->handle($message);  // 单条错误抛异常
     *     }
     *     $consumer->commitBatch();  // 整批成功，统一 commit
     * } catch (\Throwable $e) {
     *     // 不调 commitBatch → 整批下次重投
     * }
     * ```
     *
     * ## 约束
     *
     * - 必须在 `pollBatch` 之后调，否则可能 commit 错位
     * - 整批原子 commit：要么全成功要么全失败重投（v0.3 简化，不支持部分 commit）
     *
     * @return void
     */
    public function commitBatch(): void
    {
        $this->commitAsync();
    }

    /**
     * 优雅关闭。Worker 退出前必调。
     *
     * 失败时 `error_log` 静默（不抛异常），避免在退出阶段崩溃。
     *
     * @return void
     */
    public function close(): void
    {
        try {
            $this->kafka->close();
        } catch (\Throwable $e) {
            error_log('[laravel-kafka] consumer close error: ' . $e->getMessage());
        }
    }

    /**
     * 拿当前订阅描述。
     *
     * @return Subscription
     */
    public function subscription(): Subscription
    {
        return $this->subscription;
    }

    /**
     * 拿底层 `RdKafka\KafkaConsumer` 实例（业务方一般不直接用，留给扩展）。
     *
     * @return RdKafkaConsumer
     */
    public function kafka(): RdKafkaConsumer
    {
        return $this->kafka;
    }

    /**
     * 内部：把 `RdKafka\Message` 包装成 `LaravelKafka\Producer\Message`。
     *
     * ## 流程
     *
     *  1. 归一化 headers（librdkafka 返回 array / object 都有可能）
     *  2. 注入 3 个 Kafka 侧位置 header（topic / partition / offset）
     *  3. **v0.2 旧消息回退**：如果消息无 `traceparent` 但有 `x-trace-id`，
     *     自动把 16hex 短 id 升级为 32hex W3C 格式
     *  4. 构造 `LaravelKafka\Producer\Message` 值对象
     *
     * ## 旧消息回退示例
     *
     * v0.1 发的消息：
     * ```
     * x-trace-id: abc123def456
     * ```
     *
     * v0.2 消费时自动升级为：
     * ```
     * x-trace-id:    abc123def456
     * traceparent:   00-00000000000000000000000000abc123def456-<random 8hex>-01
     * ```
     *
     * @param RdKafkaMessage $rdMsg librdkafka 原始消息
     * @return Message 包装好的消息值对象
     */
    private function wrap(RdKafkaMessage $rdMsg): Message
    {
        $headers = [];
        if (is_array($rdMsg->headers)) {
            foreach ($rdMsg->headers as $k => $v) {
                $headers[(string) $k] = (string) $v;
            }
        }

        // 把当前消息的 offset 注入 header，方便后续 requeue / dlq
        $headers['x-original-topic'] = (string) $rdMsg->topic_name;
        $headers['x-original-partition'] = (string) $rdMsg->partition;
        $headers['x-original-offset'] = (string) $rdMsg->offset;

        // v0.2 兜底：旧消息（v0.1 发的，无 traceparent）回退到 x-trace-id
        if (! isset($headers[Header::TRACEPARENT]) && isset($headers[Header::TRACE_ID])) {
            $shortId = $headers[Header::TRACE_ID];
            // 把 16 hex 短 id 升级成 32 hex（前面补 16 个 0），保持 W3C 格式
            $headers[Header::TRACEPARENT] = '00-' . str_pad($shortId, 32, '0', STR_PAD_LEFT) . '-' . bin2hex(random_bytes(8)) . '-01';
        }

        return new Message(
            (string) $rdMsg->payload,
            $headers,
            $rdMsg->key !== null ? (string) $rdMsg->key : null,
            (int) $rdMsg->partition,
            (int) ($rdMsg->timestamp ?? 0),
        );
    }
}
