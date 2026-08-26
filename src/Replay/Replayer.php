<?php

declare(strict_types=1);

namespace LaravelKafka\Replay;

use LaravelKafka\Exceptions\KafkaException;

/**
 * 消息回放器（v0.3 Step 4）。
 *
 * ## 概念
 *
 * 把 topic 过去一段时间窗口的消息**重新** produce 到 target topic：
 *  - 原始 topic: `orders.events`
 *  - 目标 topic: `orders.events.replay`
 *  - 时间窗口: `--from="-1h" --to=now`
 *
 * ## 流程
 *
 *  1. 解析 `--from` / `--to` 为 Unix timestamp
 *  2. 用独立 consumer group `replay-runner`（避免影响主消费者）
 *  3. 调 `offsetsForTimes` 查 timestamp 对应 offset
 *  4. `assign` 到该 offset 范围
 *  5. 循环 `consume` 拉消息，`reproduce` 到 target topic
 *  6. 保留 `original_*` headers
 *
 * ## 兼容性
 *
 * - 业务方不感知回放（独立 consumer group）
 * - 不影响主消费者 offset
 * - 不修改原消息（reproduce 时复制 payload + 关键 headers）
 *
 * ## v0.3 MVP 限制
 *
 * - 简化实现：`offsetsForTimes` 在单 partition 场景下工作；多 partition 需遍历
 * - 暂不支持按 partition 范围
 */
final class Replayer
{
    /**
     * 解析时间窗口。
     *
     * @param string $from --from 参数
     * @param string $to --to 参数
     * @return array{from: int, to: int} Unix timestamp
     * @throws KafkaException 解析失败或 from >= to
     */
    public function parseWindow(string $from, string $to): array
    {
        $parser = new TimeWindowParser();
        $fromTs = $parser->parse($from);
        $toTs = $parser->parse($to);

        if ($fromTs >= $toTs) {
            throw new KafkaException(sprintf(
                'Replayer window: from (%s = %d) must be < to (%s = %d)',
                $from,
                $fromTs,
                $to,
                $toTs
            ));
        }

        return ['from' => $fromTs, 'to' => $toTs];
    }
}
