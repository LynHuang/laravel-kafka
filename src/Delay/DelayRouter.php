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
 * ## 完整工作流（本文件在其中的角色）
 *
 * ```
 * ① 生产端                       ② 延迟 worker                       ③ 业务消费者
 * Queue::later(30s, job)         kafka:delay:work                   主 topic 消费者
 *      │                              │                                  ▲
 *      ▼  route(30) → "delay-30s"     ▼ 监听所有 tier topic               │
 * 写消息到 "delay-30s"  ────────►  时间到 → requeue 到主 topic ────────────┘
 *     （带 delay_seconds / x-original-queue 等 header）
 * ```
 *
 * - **① 生产端**：`KafkaQueue::later()` 调用本类的 `route($delay)` 算出应投递的 tier topic，
 *   并把原始队列名（`x-original-queue`）一并写入 header；
 * - **② 延迟 worker**：`kafka:delay:work` 用 `allTopics()` 拿到全部 tier topic 并监听，
 *   消息到期后用 `parseTier()` 反向解析所属 tier，再 requeue 回主 topic；
 * - **③ 业务消费者**：只监听主 topic，完全无感知，拿到的就是"到期的"消息。
 *
 * 本类只负责**路由计算与配置校验**（纯逻辑、无 I/O），是延迟机制的地基：
 * `route()` 决定消息"存哪"，`allTopics()` / `parseTier()` 决定 worker"听哪 / 怎么还原"。
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
        // 先复制一份再排序，避免 sort() 原地修改污染原数组
        $sorted = $tiers;
        sort($sorted);
        // 排序后与原数组不一致 → 说明配置不是升序，直接抛异常
        // 用 !== 严格比较（同时校验元素类型），顺序不符即报错
        if ($sorted !== $tiers) {
            throw new KafkaException(sprintf(
                'DelayRouter tiers must be in ascending order, got: [%s]',
                implode(', ', array_map('strval', $tiers))
            ));
        }
        // 逐项校验 tier 必须 > 0，防止配置错误导致路由到非法 topic
        foreach ($tiers as $tier) {
            if ($tier <= 0) {
                throw new KafkaException(sprintf(
                    'DelayRouter tier must be > 0, got: %d',
                    $tier
                ));
            }
        }
        // 校验通过后写入成员属性，供 route() / allTopics() 使用
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
        // 延迟必须 > 0，否则没有"排队"意义，直接拒绝
        if ($delaySeconds <= 0) {
            throw new KafkaException(sprintf(
                'DelayRouter delay must be > 0, got: %d',
                $delaySeconds
            ));
        }

        // 核心路由逻辑：取"第一个 ≥ delay 的 tier"（向上取整到最近一层）
        //
        // 例：tiers = [5, 30, 60, 300, ...]
        //   delay=3s   → 5s    （5 是第一个 ≥ 3 的 tier）
        //   delay=30s  → 30s   （恰好命中，无需进位）
        //   delay=45s  → 60s   （45 > 30，进位到 60）
        //
        // 为什么"向上取整"而不是取最大 tier？
        // 延迟是"至少等这么久"，选最近一层能把延迟误差控制到最小——
        // 选 5s 槽的 delay=3s 只多等 2s；若一律塞进 86400s 槽，3s 的消息要等一天。
        //
        // 兜底：$this->tiers[0] 保证 tier 变量始终有值（tiers 非空由构造函数校验）。
        $tier = $this->tiers[0];
        foreach ($this->tiers as $candidate) {
            if ($delaySeconds <= $candidate) {
                $tier = $candidate;
                break;
            }
        }

        // 超过最大 tier → 用最大 tier（不限制最大延迟，业务方知道自己在干嘛）
        // 例：delay=90000s(25h) 超过 86400s → 塞进 86400s 槽，虽然误差大，
        // 但至少消息不会丢；这是"分层"方案对超大延迟的已知取舍。
        $last = end($this->tiers);
        if ($delaySeconds > $last) {
            $tier = $last;
        }

        // 返回目标 topic 名（如 "delay-30s"）和 tier 秒数，
        // 生产端直接往该 topic 写消息即可
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
        // 由 kafka:delay:work 启动时调用：
        // 拿到全部 tier topic 列表，worker 才能为每个层级各起一个监听器
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
        // 只读访问器：给外部（如监控/调试命令）看当前配置了哪些 tier，
        // 返回的是构造函数校验过的升序数组，不会暴露内部可变性
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
        // 反向解析：worker 收到一条 tier topic 的消息时，
        // 用它还原"这条消息属于哪个 tier 槽"，进而算出还要等多久、该 requeue 到哪
        //
        // 例：topic="delay-30s" → 30
        $prefix = $this->topicPrefix . '-';
        // 前缀对不上 → 不是本路由生成的 tier topic，返回 0 表示"无法识别"
        if (strpos($topic, $prefix) !== 0) {
            return 0;
        }
        // 去掉前缀（"delay-"），要求尾部必须是 "s" 单位后缀
        $rest = substr($topic, strlen($prefix));
        if (substr($rest, -1) !== 's') {
            return 0;
        }
        // 去掉 "s" 后强转整数（如 "30s" → 30），再兜底：≤ 0 视为非法
        $tier = (int) substr($rest, 0, -1);
        return $tier > 0 ? $tier : 0;
    }
}
