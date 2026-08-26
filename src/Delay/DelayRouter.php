<?php

declare(strict_types=1);

namespace LaravelKafka\Delay;

use LaravelKafka\Exceptions\KafkaException;

/**
 * 时间轮分层路由（v0.3 Step 2）。
 *
 * ## 概念
 *
 * Kafka 不原生支持延迟消息。本实现用"分层 topic"模拟时间轮：
 *
 * ```
 *  delay=5s   → 写 "delay-5s"   topic, 5s 后消费者 requeue 到主 topic
 *  delay=30s  → 写 "delay-30s"  topic, 30s 后消费者 requeue
 *  delay=60s  → 写 "delay-60s"  topic
 *  ...
 * ```
 *
 * ## 路由规则（v0.1 §4.5 方案 A）
 *
 * 给定 delay 选**最近一层**（向上取整）：
 * - delay=3s   → tier 5s   (5s 槽)
 * - delay=10s  → tier 30s  (30s 槽)
 * - delay=45s  → tier 60s
 * - delay=400s → tier 1800s
 * - delay=90000s → tier 86400s (1 day 兜底)
 *
 * ## 配置
 *
 * `kafka.php`:
 * ```php
 * 'delay' => [
 *     'tiers' => [5, 30, 60, 300, 1800, 3600, 86400],  // 8 个 tier
 *     'topic_prefix' => 'delay',                         // 生成 "delay-5s" 等
 * ],
 * ```
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 用 `Kafka::later($seconds, $payload)` 单 topic + 内部定时器轮询（依赖内存状态，重启丢）。
 * 我们用分层 topic（broker 持久化，worker 重启不丢延迟消息）。
 */
final class DelayRouter
{
    /**
     * 升序排列的 tier 秒数。
     *
     * @var array<int, int>
     */
    private array $tiers;

    /**
     * topic 前缀（生成的 tier topic = "$prefix-{$tier}s"）。
     */
    private string $topicPrefix;

    /**
     * @param array<int, int> $tiers tier 秒数（升序，业务方配置）
     * @param string $topicPrefix topic 前缀（默认 "delay"）
     * @throws KafkaException tier 配置非法时
     */
    public function __construct(array $tiers, string $topicPrefix = 'delay')
    {
        // 校验：必须升序 + 全部 > 0
        $sorted = $tiers;
        sort($sorted);
        if ($sorted !== $tiers) {
            throw new KafkaException(sprintf(
                'DelayRouter tiers must be in ascending order, got: [%s]',
                implode(', ', array_map('strval', $tiers))
            ));
        }
        foreach ($tiers as $tier) {
            if ($tier <= 0) {
                throw new KafkaException(sprintf(
                    'DelayRouter tier must be > 0, got: %d',
                    $tier
                ));
            }
        }
        $this->tiers = $tiers;
        $this->topicPrefix = $topicPrefix;
    }

    /**
     * 给定 delay 选最近一层的 tier topic 名 + tier 值。
     *
     * @param int $delaySeconds 延迟秒数（必须 > 0）
     * @return array{topic: string, tier: int} topic 名 + tier 值
     * @throws KafkaException delay <= 0 时
     */
    public function route(int $delaySeconds): array
    {
        if ($delaySeconds <= 0) {
            throw new KafkaException(sprintf(
                'DelayRouter delay must be > 0, got: %d',
                $delaySeconds
            ));
        }

        $tier = $this->tiers[0];
        foreach ($this->tiers as $candidate) {
            if ($delaySeconds <= $candidate) {
                $tier = $candidate;
                break;
            }
        }

        // 超过最大 tier → 用最大 tier（不限制最大延迟，业务方知道自己在干嘛）
        $last = end($this->tiers);
        if ($delaySeconds > $last) {
            $tier = $last;
        }

        return [
            'topic' => $this->topicPrefix . '-' . $tier . 's',
            'tier' => $tier,
        ];
    }

    /**
     * 拿到所有 tier topic 列表（用于 `kafka:delay:work` 启动监听）。
     *
     * @return array<int, string> topic 名列表
     */
    public function allTopics(): array
    {
        $topics = [];
        foreach ($this->tiers as $tier) {
            $topics[] = $this->topicPrefix . '-' . $tier . 's';
        }
        return $topics;
    }

    /**
     * 拿到所有 tier 秒数（升序）。
     *
     * @return array<int, int>
     */
    public function tiers(): array
    {
        return $this->tiers;
    }

    /**
     * 从 tier topic 名提取 tier 秒数（反向解析）。
     *
     * @param string $topic tier topic 名（如 "delay-5s"）
     * @return int tier 秒数（找不到返回 0）
     */
    public function parseTier(string $topic): int
    {
        $prefix = $this->topicPrefix . '-';
        if (strpos($topic, $prefix) !== 0) {
            return 0;
        }
        $rest = substr($topic, strlen($prefix));
        if (substr($rest, -1) !== 's') {
            return 0;
        }
        $tier = (int) substr($rest, 0, -1);
        return $tier > 0 ? $tier : 0;
    }
}
