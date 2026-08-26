<?php

declare(strict_types=1);

namespace LaravelKafka\Queue\Failed;

use LaravelKafka\Exceptions\KafkaException;
use Throwable;

/**
 * 异常类 → DLQ topic 路由（v0.3 Step 3）。
 *
 * ## 用途
 *
 * v0.1/v0.2 所有失败消息写**单一** DLQ topic（`<topic>.dlq`）。
 * 业务场景：不同异常需要不同处理：
 *  - `App\Exception\BizFatalException` → "fatal-biz" topic（人工处理）
 *  - `App\Exception\NetworkTimeoutException` → "transient-net" topic（自动重试）
 *  - `InvalidArgumentException` → "validation" topic（开发调试）
 *
 * ## 路由规则
     *
     * 读 `kafka.connections.{name}.failed.dlq.topic_map`（`exception class` → `dlq topic`）：
     * ```php
     * 'topic_map' => [
     *     'App\\Exception\\BizFatalException' => 'laravel-jobs.dlq.fatal',
     *     'App\\Exception\\NetworkTimeoutException' => 'laravel-jobs.dlq.transient',
     * ],
     * ```
     *
     * 匹配规则：
     *  - 精确匹配（`instanceof` 是 a-kind-of 关系）
     *  - 找不到匹配 → 用 `default_topic`（来自 `failed.dlq.topic` 或 `auto_topic_suffix`）
 */
final class ExceptionClassRouter
{
    /**
     * 异常类名 → DLQ topic 映射。
     *
     * @var array<string, string>
     */
    private array $topicMap;

    /**
     * 兜底 DLQ topic（找不到匹配时用）。
     */
    private string $defaultTopic;

    /**
     * @param array<string, string> $topicMap 异常类名 → DLQ topic 映射
     * @param string $defaultTopic 兜底 topic
     */
    public function __construct(array $topicMap, string $defaultTopic)
    {
        if ($defaultTopic === '') {
            throw new KafkaException('ExceptionClassRouter defaultTopic must not be empty.');
        }
        $this->topicMap = $topicMap;
        $this->defaultTopic = $defaultTopic;
    }

    /**
     * 给定异常，返回 DLQ topic 名。
     *
     * 匹配规则：遍历 `$topicMap` 找第一个 `$exception instanceof $class` 命中的。
     * 找不到 → 兜底 topic。
     *
     * @param Throwable $exception 失败抛出的异常
     * @return string DLQ topic 名
     */
    public function route(Throwable $exception): string
    {
        foreach ($this->topicMap as $className => $topic) {
            if ($exception instanceof $className) {
                return (string) $topic;
            }
        }
        return $this->defaultTopic;
    }

    /**
     * 拿到兜底 topic。
     *
     * @return string
     */
    public function defaultTopic(): string
    {
        return $this->defaultTopic;
    }

    /**
     * 拿到所有映射条目数。
     *
     * @return int
     */
    public function size(): int
    {
        return count($this->topicMap);
    }
}
