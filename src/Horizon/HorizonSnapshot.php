<?php

declare(strict_types=1);

namespace LaravelKafka\Horizon;

use Carbon\CarbonImmutable;

/**
 * Horizon 兼容的 metrics 快照（v0.4 单任务）。
 *
 * ## 角色
 *
 * 定时把当前 metrics 移到 `snapshot:queue:<name>` Sorted Set，保留历史 N 份（默认 24）。
 * Horizon 5.x 的 `RedisMetricsRepository::snapshot()` 行为一致：
 *  - `hmget(queue:X, throughput, runtime)` → 读当前 metrics
 *  - `zadd(snapshot:queue:X, time, json_data)` → 写历史
 *  - `del(queue:X)` → 清当前（下次 increment 重新累计）
 *  - `zremrangebyrank(snapshot:queue:X, 0, -N-1)` → 保留最新 N 份
 *
 * ## 用法
 *
 * 业务方在 `app/Console/Kernel.php` 加定时任务：
 * ```php
 * $schedule->call(function () {
 *     app(\LaravelKafka\Horizon\HorizonSnapshot::class)->snapshot();
 * })->everyMinute();
 * ```
 *
 * 或跑 `kafka:horizon:snapshot` 命令（v0.4.0 提供）。
 */
final class HorizonSnapshot
{
    /**
     * 队列名 → 历史快照保留数（默认 24 = 24 分钟）。
     */
    private int $trimSnapshots;

    /**
     * 作业名 → 历史快照保留数（默认 24 = 24 分钟）。
     */
    private int $trimSnapshotsJob;

    /**
     * @param int $trimSnapshots queue 快照保留数（默认 24）
     * @param int $trimSnapshotsJob job 快照保留数（默认 24）
     */
    public function __construct(int $trimSnapshots = 24, int $trimSnapshotsJob = 24)
    {
        $this->trimSnapshots = $trimSnapshots;
        $this->trimSnapshotsJob = $trimSnapshotsJob;
    }

    /**
     * 给单个 queue 写一份快照 + 清当前计数器。
     *
     * @param mixed $conn Redis connection
     * @param string $prefix Horizon key 前缀
     * @param string $queue queue 名
     * @return void
     */
    public function snapshotQueue($conn, string $prefix, string $queue): void
    {
        $hashKey = $prefix . 'queue:' . $queue;
        $snapshotKey = $prefix . 'snapshot:queue:' . $queue;
        $time = CarbonImmutable::now()->getTimestamp();

        // 1. 读 + 删当前 metrics（事务式）
        $values = $conn->transaction(function ($trans) use ($hashKey) {
            $trans->hmget($hashKey, ['throughput', 'runtime']);
            $trans->del($hashKey);
            return $trans->execute();
        });

        $throughput = $values[0] ?? 0;
        $runtime = $values[1] ?? 0;

        // 2. 写快照
        $conn->zadd($snapshotKey, $time, json_encode([
            'throughput' => $throughput,
            'runtime' => $runtime,
            'time' => $time,
        ]));

        // 3. 保留最新 N 份
        $conn->zremrangebyrank($snapshotKey, 0, -abs(1 + $this->trimSnapshots));
    }
}
