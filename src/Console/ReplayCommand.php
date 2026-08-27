<?php

declare(strict_types=1);

namespace LaravelKafka\Console;

use Illuminate\Console\Command;
use LaravelKafka\Replay\Replayer;
use LaravelKafka\Replay\ReplayExecutor;

/**
 * `php artisan kafka:replay` 命令（v0.3 Step 4，v0.5.3 实现实际 reproduce）。
 *
 * ## 业务方使用
 *
 * ```bash
 * # 把 orders.events 过去 1 小时的消息重新推到 orders.events.replay
 * php artisan kafka:replay \
 *     --topic=orders.events \
 *     --from="-1h" \
 *     --to=now \
 *     --target-topic=orders.events.replay \
 *     --group=replay-runner
 * ```
 *
 * ## v0.5.3 实现
 *
 * - 时间窗口解析 + 参数校验（v0.3 已有）
 * - **实际 reproduce**（v0.5.3 新增）：`offsetsForTimes` 找 offset → 遍历 partition
 *   → `Producer::send` 重放到 target-topic
 */
final class ReplayCommand extends Command
{
    /**
     * @var string
     */
    protected $signature = 'kafka:replay
        {--topic= : 源 topic}
        {--from= : 起始时间（-1h / now / 1700000000 / 2026-08-25 10:00:00）}
        {--to= : 结束时间（同 from）}
        {--target-topic= : 目标 topic}
        {--group=replay-runner : 独立 consumer group，不影响主消费者}';

    /**
     * @var string
     */
    protected $description = '把源 topic 时间窗口内的消息重新 produce 到目标 topic';

    public function handle(Replayer $replayer, ReplayExecutor $executor): int
    {
        $topic = (string) $this->option('topic');
        $from = (string) $this->option('from');
        $to = (string) $this->option('to');
        $targetTopic = (string) $this->option('target-topic');
        $group = (string) $this->option('group');

        // 参数校验
        if ($topic === '' || $from === '' || $to === '' || $targetTopic === '') {
            $this->error('--topic, --from, --to, --target-topic are required');
            return 1;
        }

        try {
            $window = $replayer->parseWindow($from, $to);
        } catch (\Throwable $e) {
            $this->error('Window parse failed: ' . $e->getMessage());
            return 1;
        }

        $this->info(sprintf(
            '[kafka:replay] topic=%s window=[%s (%d) → %s (%d)] target=%s group=%s',
            $topic,
            $from,
            $window['from'],
            $to,
            $window['to'],
            $targetTopic,
            $group
        ));

        // v0.5.3: 实际 reproduce
        $config = $this->laravel->make('kafka.manager')->config();
        try {
            $result = $executor->execute(
                $topic,
                $window['from'],
                $window['to'],
                $targetTopic,
                $group,
                $config
            );
        } catch (\Throwable $e) {
            $this->error('[kafka:replay] reproduce failed: ' . $e->getMessage());
            return 1;
        }

        $this->info(sprintf(
            '[kafka:replay] done: replayed %d message(s) from %d partition(s) to "%s"',
            $result['replayed'],
            $result['partitions'],
            $targetTopic
        ));

        return 0;
    }
}
