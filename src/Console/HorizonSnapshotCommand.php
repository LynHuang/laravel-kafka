<?php

declare(strict_types=1);

namespace LaravelKafka\Console;

use Illuminate\Console\Command;
use LaravelKafka\Horizon\HorizonMetricsRecorder;
use LaravelKafka\Horizon\HorizonSnapshot;

/**
 * `php artisan kafka:horizon:snapshot` 命令（v0.4）。
 *
 * ## 用途
 *
 * 把当前 Kafka 队列 metrics 快照到 Redis（Horizon 兼容格式）。
 * 业务方在 `app/Console/Kernel.php` 加定时任务：
 * ```php
 * $schedule->command('kafka:horizon:snapshot')->everyMinute();
 * ```
 *
 * ## 选项
 *
 * - `--connection=horizon` —— Redis 连接名
 * - `--prefix=horizon:` —— Redis key 前缀
 * - `--trim=24` —— queue 快照保留数
 *
 * ## 与 Horizon 原生命令的关系
 *
 * - Horizon 自带 `horizon:snapshot`（业务方如果同时装 Horizon，可直接用）
 * - 本命令是**独立**的（不依赖 Horizon 包），适合**只装 laravel-kafka + Redis** 的项目
 */
final class HorizonSnapshotCommand extends Command
{
    /**
     * @var string
     */
    protected $signature = 'kafka:horizon:snapshot
        {--connection=horizon : Redis 连接名（与 Horizon config 同步）}
        {--prefix=horizon: : Redis key 前缀}
        {--trim=24 : 每个 queue 保留的快照数}
        {--trim-job=24 : 每个 job 保留的快照数}';

    /**
     * @var string
     */
    protected $description = '把 Kafka 队列 metrics 快照到 Redis（Horizon 兼容格式）';

    public function handle(): int
    {
        $this->info('[kafka:horizon:snapshot] v0.4.4: 真跑 snapshot (v0.4.0-0.4.3 是 stub).');

        $connection = (string) $this->option('connection');
        $prefix = (string) $this->option('prefix');
        $trim = (int) $this->option('trim');
        $trimJob = (int) $this->option('trim-job');

        // 拿 Redis Factory
        $factory = $this->laravel->make(\Illuminate\Contracts\Redis\Factory::class);
        $conn = $factory->connection($connection);

        // v0.4.4: Laravel PhpRedisConnection 自动加 prefix (e.g. 'laravel_database_'),
        // smembers 拿到的 key 实际是 'laravel_database_<prefix>queue:xxx'.
        // snapshotQueue 调 phpredis 又会**再加一遍** prefix → 双重 prefix bug.
        // 修法: 拿到 key 后手动 strip Laravel prefix, snapshotQueue 内部再加回.
        $laravelPrefix = '';
        if ($conn instanceof \Illuminate\Redis\Connections\PhpRedisConnection) {
            $laravelPrefix = (string) $conn->client()->getOption(\Redis::OPT_PREFIX);
        }

        // 拿 measured_queues / measured_jobs Set 的成员
        $queueMembers = $this->scanSet($conn, $prefix . 'measured_queues');
        $jobMembers = $this->scanSet($conn, $prefix . 'measured_jobs');

        $snapshot = new \LaravelKafka\Horizon\HorizonSnapshot($trim, $trimJob);

        $countQueues = 0;
        foreach ($queueMembers as $fullKey) {
            // strip Laravel prefix + strip 'queue:' 前缀
            $stripped = (string) $fullKey;
            if ($laravelPrefix !== '' && strpos($stripped, $laravelPrefix) === 0) {
                $stripped = substr($stripped, strlen($laravelPrefix));
            }
            // stripped 现在应该是 '<prefix>queue:emails'
            $queue = substr($stripped, strlen($prefix . 'queue:'));
            $snapshot->snapshotQueue($conn, $prefix, $queue);
            $countQueues++;
        }

        $countJobs = 0;
        foreach ($jobMembers as $fullKey) {
            $stripped = (string) $fullKey;
            if ($laravelPrefix !== '' && strpos($stripped, $laravelPrefix) === 0) {
                $stripped = substr($stripped, strlen($laravelPrefix));
            }
            $job = substr($stripped, strlen($prefix . 'job:'));
            $snapshot->snapshotQueue($conn, $prefix, $job);
            $countJobs++;
        }

        $this->info(sprintf(
            '[kafka:horizon:snapshot] snapshotted %d queue(s), %d job(s) to prefix="%s" connection="%s"',
            $countQueues,
            $countJobs,
            $prefix,
            $connection
        ));

        $this->warn('业务方应同时启用 Horizon 自身 snapshot 任务（如果已装 Horizon）。');

        return 0;
    }

    /**
     * 内部：拿 Redis Set 的成员。
     *
     * @param mixed $conn
     * @param string $setKey
     * @return string[]
     */
    private function scanSet($conn, string $setKey): array
    {
        $members = (array) $conn->smembers($setKey);
        return $members;
    }
}
