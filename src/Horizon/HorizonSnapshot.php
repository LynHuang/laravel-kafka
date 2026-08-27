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
     * v0.4.4 hotfix: 原实现 `$conn->transaction(fn => $trans->execute())` 用 predis 风格,
     *                phpredis 实际方法是 `exec()` 不是 `execute()`, 抛 'undefined method Redis::execute()'.
     * 修法: 不用 transaction 包装, 直接 sequential 调用 (hmget + del), 接受 race condition
     *        (snapshot 跑在后台, 业务方短时双写不影响 metrics).
     *
     * @param mixed $conn Redis connection (Laravel PhpRedisConnection / PredisConnection / raw phpredis)
     * @param string $prefix Horizon key 前缀
     * @param string $queue queue 名
     * @return void
     */
    public function snapshotQueue($conn, string $prefix, string $queue): void
    {
        $hashKey = $prefix . 'queue:' . $queue;
        $snapshotKey = $prefix . 'snapshot:queue:' . $queue;
        $time = CarbonImmutable::now()->getTimestamp();

        // 1. 读 + 删当前 metrics (race-tolerant: 业务方 snapshot 跑在后台, 短时双写可接受)
        $values = $conn->hmget($hashKey, ['throughput', 'runtime']);
        $conn->del($hashKey);

        $throughput = is_array($values) ? ((int) ($values[0] ?? 0)) : 0;
        $runtime = is_array($values) ? ((float) ($values[1] ?? 0)) : 0.0;

        // 2. 写快照
        $conn->zadd($snapshotKey, $time, json_encode([
            'throughput' => $throughput,
            'runtime' => $runtime,
            'time' => $time,
        ]));

        // 3. 保留最新 N 份
        $conn->zremrangebyrank($snapshotKey, 0, -abs(1 + $this->trimSnapshots));
    }

    /**
     * 给单个 job 写一份快照 + 清当前计数器。
     *
     * v0.4.6 新增（修复 v0.4.0-0.4.5 错把 job 路径走 `snapshotQueue` 的 bug）：
     * 之前 {@see HorizonSnapshotCommand} 处理 measured_jobs set 时也调 `snapshotQueue`,
     * 导致 Redis key 写成 `<prefix>queue:<className>` + `<prefix>snapshot:queue:<className>`,
     * 不是 Horizon 期望的 `<prefix>job:<className>` + `<prefix>snapshot:job:<className>`.
     *
     * ## 行为
     *
     *  1. `hmget(prefix + job:<job>, throughput, runtime)` → 读当前 metrics
     *  2. `del(prefix + job:<job>)` → 清当前（下次 increment 重新累计）
     *  3. `zadd(prefix + snapshot:job:<job>, time, json_data)` → 写历史
     *  4. `zremrangebyrank(snapshot:job:<job>, 0, -N-1)` → 保留最新 N 份
     *
     * @param mixed $conn Redis connection
     * @param string $prefix Horizon key 前缀
     * @param string $jobClass job 完整类名（如 `App\Jobs\OrderJob`）
     * @return void
     */
    public function snapshotJob($conn, string $prefix, string $jobClass): void
    {
        $hashKey = $prefix . 'job:' . $jobClass;
        $snapshotKey = $prefix . 'snapshot:job:' . $jobClass;
        $time = CarbonImmutable::now()->getTimestamp();

        // 1. 读 + 删当前 metrics (race-tolerant: snapshot 跑在后台, 业务方短时双写可接受)
        $values = $conn->hmget($hashKey, ['throughput', 'runtime']);
        $conn->del($hashKey);

        $throughput = is_array($values) ? ((int) ($values[0] ?? 0)) : 0;
        $runtime = is_array($values) ? ((float) ($values[1] ?? 0)) : 0.0;

        // 2. 写快照
        $conn->zadd($snapshotKey, $time, json_encode([
            'throughput' => $throughput,
            'runtime' => $runtime,
            'time' => $time,
        ]));

        // 3. 保留最新 N 份 (job 单独用 trimSnapshotsJob)
        $conn->zremrangebyrank($snapshotKey, 0, -abs(1 + $this->trimSnapshotsJob));
    }
}
