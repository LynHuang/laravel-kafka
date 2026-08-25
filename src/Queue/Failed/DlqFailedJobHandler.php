<?php

declare(strict_types=1);

namespace LaravelKafka\Queue\Failed;

use Illuminate\Contracts\Events\Dispatcher;
use LaravelKafka\Events\MessageSentToDLQ;
use LaravelKafka\Producer\Message;
use LaravelKafka\Producer\Producer;
use LaravelKafka\Queue\KafkaJob;
use LaravelKafka\Support\Header;
use Throwable;

/**
 * 把失败任务写入 DLQ topic。
 *
 * ## DLQ 消息额外 header（9 个）
 *
 * 在原消息 headers 基础上追加：
 *
 * | Header | 含义 |
 * | --- | --- |
 * | `x-original-topic` | 源 topic |
 * | `x-original-partition` | 源 partition |
 * | `x-original-headers` | 原 headers 的 JSON 序列化 |
 * | `x-failed-at` | 失败时间 (ms) |
 * | `x-exception-class` | 异常类名 |
 * | `x-exception-message` | 异常 message（按 config 截断） |
 * | `x-exception-trace` | 异常 trace（按 config 截断） |
 * | `x-attempts` | 累计重试次数 |
 * | `x-job-id` | 业务方给 Job 分配的 id（如果有） |
 * | `x-queue` / `x-connection` | Laravel 逻辑队列 / connection |
 *
 * ## payload 透传
 *
 * **关键**：DLQ 消息的 payload = 原消息的 payload，**不**重新序列化。
 * 跨语言消费 DLQ 时能直接拿到原字节流。
 *
 * ## v0.2 事件
 *
 * DLQ 写入**成功后** dispatch {@see MessageSentToDLQ} 事件（业务方做告警 / metrics）。
 */
final class DlqFailedJobHandler implements FailedJobHandlerInterface
{
    /**
     * 真实生产者（封装 librdkafka Conf + RdKafka\Producer）。
     *
     * @var Producer
     */
    private Producer $producer;

    /**
     * DLQ topic 名（解析后）。
     *
     * @var string
     */
    private string $dlqTopic;

    /**
     * DLQ 配置数组（从 `kafka.connections.default.failed.dlq` 读）。
     *
     * @var array<string,mixed>
     */
    private array $config;

    /**
     * 构造时注入所有依赖。
     *
     * @param Producer $producer  真实生产者
     * @param string $dlqTopic    DLQ topic 名
     * @param array<string,mixed> $config DLQ 配置（含 truncate 字节数等）
     */
    public function __construct(Producer $producer, string $dlqTopic, array $config = [])
    {
        $this->producer = $producer;
        $this->dlqTopic = $dlqTopic;
        $this->config = $config;
    }

    /**
     * 把失败任务写入 DLQ topic。
     *
     * ## 流程
     *
     *  1. 读 truncate 字节数（默认 message=4KB, trace=32KB）
     *  2. 合并原 headers + 9 个 DLQ 专属 header
     *  3. 用原 payload 构造 Message（**不**重新序列化）
     *  4. Producer::send 推到 DLQ topic
     *  5. dispatch {@see MessageSentToDLQ} 事件
     *
     * @param KafkaJob $job       失败的 Laravel Job（含 connection / queue / jobId）
     * @param Throwable $exception 业务抛出的异常
     * @param FailedContext $context 失败上下文（payload / headers / topic / partition / attempts）
     * @return void
     */
    public function handle(KafkaJob $job, Throwable $exception, FailedContext $context): void
    {
        $messageTruncate = (int) ($this->config['message_truncate_bytes'] ?? 4096);
        $traceTruncate = (int) ($this->config['trace_truncate_bytes'] ?? 32768);

        $headers = array_merge(
            $context->headers(),
            [
                'x-original-topic' => $context->topic(),
                'x-original-partition' => (string) $context->partition(),
                'x-original-headers' => json_encode(
                    $context->headers(),
                    JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES
                ),
                'x-failed-at' => (string) (int) (microtime(true) * 1000),
                'x-exception-class' => get_class($exception),
                'x-exception-message' => $this->truncate($exception->getMessage(), $messageTruncate),
                'x-exception-trace' => $this->truncate($exception->getTraceAsString(), $traceTruncate),
                'x-attempts' => (string) $context->attempts(),
                'x-job-id' => (string) $job->getJobId(),
                'x-queue' => $job->getQueue(),
                'x-connection' => $job->getConnectionName(),
            ]
        );

        // 关键：payload 与原消息完全相同，不重新序列化
        $message = new Message(
            $context->rawPayload(),
            $headers,
            $context->headers()[Header::TRACE_ID] ?? null,
        );

        $this->producer->send($this->dlqTopic, $message);

        // v0.2 引入：DLQ 写入后 dispatch 事件
        $this->dispatchEvent(new MessageSentToDLQ($this->dlqTopic, $message, $exception));
    }

    /**
     * 内部：dispatch Laravel 事件（容器未绑 Dispatcher 时静默跳过）。
     *
     * 与 {@see \LaravelKafka\Queue\KafkaQueue::dispatchEvent()} 不同——本类不在
     * Queue 继承链上，没有 `container` 字段，直接拿 `Container::getInstance()` 全局。
     *
     * @param object $event Laravel 事件实例
     * @return void
     */
    private function dispatchEvent(object $event): void
    {
        $container = \Illuminate\Container\Container::getInstance();
        if (! $container->bound(Dispatcher::class)) {
            return;
        }
        $container->make(Dispatcher::class)->dispatch($event);
    }

    /**
     * 内部：按字节数截断字符串（避免 Kafka header 过大）。
     *
     * 注意：按**字节**截断，不是按字符——Kafka header 是字节流。
     * 对 UTF-8 多字节字符（如中文 / emoji）可能产生半个字，v0.2 接受。
     *
     * @param string $value    原字符串
     * @param int $maxBytes 最大字节数
     * @return string 截断后的字符串（带 `... [truncated]` 后缀）
     */
    private function truncate(string $value, int $maxBytes): string
    {
        if (strlen($value) <= $maxBytes) {
            return $value;
        }
        return substr($value, 0, $maxBytes) . '... [truncated]';
    }
}
