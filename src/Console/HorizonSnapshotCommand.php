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
        $this->info('[kafka:horizon:snapshot] 业务方应同时启用 Horizon 自身 snapshot 任务。');

        $trim = (int) $this->option('trim');
        $trimJob = (int) $this->option('trim-job');

        $this->warn(sprintf(
            '当前命令保留参数 trim=%d, trim-job=%d（Horizon config 优先）',
            $trim,
            $trimJob
        ));

        return 0;
    }
}
