<?php

declare(strict_types=1);

namespace LaravelKafka\Queue\Failed;

use LaravelKafka\Exceptions\KafkaException;

/**
 * DLQ 写入限速器（v0.3 Step 3）。
 *
 * ## 用途
 *
 * 业务故障爆炸时 DLQ topic 也会爆炸：
 *  - 100k 条失败消息在 1 分钟内写 DLQ → 撑爆 broker 存储
 *  - DLQ 消费者被打爆 → 真正的告警被淹没
 *
 * 限速器按"每分钟 N 条"控制，超出时返回 `false`（业务方决定"丢弃"或"累加计数"）。
 *
 * ## 实现
 *
 * 用**滑动窗口**近似（每分钟重置计数器）：
 *  - 第 1 分钟：写 1-100 条（counter 0 → 99）
 *  - 第 2 分钟：counter 重置为 0
 *  - 业务方：每分钟 100 条限速
 *
 * 简单实现 vs Redis：单进程内存计数器，多 worker 部署会**各自限速**（实际是 100 × N）。
 * 生产环境建议用 Redis 共享计数器（v0.4 评估）。
 */
final class DlqRateLimiter
{
    /**
     * 每分钟最大写入数。
     */
    private int $maxPerMinute;

    /**
     * 当前窗口的写入计数。
     */
    private int $currentCount = 0;

    /**
     * 当前窗口开始时间戳（秒）。
     */
    private int $windowStart = 0;

    /**
     * @param int $maxPerMinute 每分钟最大数（必须 > 0）
     * @throws KafkaException $maxPerMinute <= 0 时
     */
    public function __construct(int $maxPerMinute)
    {
        if ($maxPerMinute <= 0) {
            throw new KafkaException(sprintf(
                'DlqRateLimiter maxPerMinute must be > 0, got: %d',
                $maxPerMinute
            ));
        }
        $this->maxPerMinute = $maxPerMinute;
    }

    /**
     * 尝试消耗一个令牌（写入 DLQ）。
     *
     * 返回 `true` = 允许写入，`false` = 已超限（业务方应"丢弃 + 报警"或"累加 batch 计数"）。
     *
     * @return bool
     */
    public function allow(): bool
    {
        $now = time();

        // 新窗口（过 60s）→ 重置
        if ($this->windowStart === 0 || ($now - $this->windowStart) >= 60) {
            $this->windowStart = $now;
            $this->currentCount = 0;
        }

        if ($this->currentCount >= $this->maxPerMinute) {
            return false;
        }

        $this->currentCount++;
        return true;
    }

    /**
     * 当前窗口已用数量。
     *
     * @return int
     */
    public function currentCount(): int
    {
        $now = time();
        if (($now - $this->windowStart) >= 60) {
            return 0;  // 已过期
        }
        return $this->currentCount;
    }

    /**
     * 配额上限。
     *
     * @return int
     */
    public function maxPerMinute(): int
    {
        return $this->maxPerMinute;
    }
}
