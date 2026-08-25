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
