<?php

declare(strict_types=1);

namespace LaravelKafka\Console;

use Illuminate\Console\Command;
use LaravelKafka\Replay\Replayer;

/**
 * `php artisan kafka:replay` 命令（v0.3 Step 4）。
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
 * ## v0.3 MVP 限制
 *
 * - 不实际调 librdkafka `offsetsForTimes`（留 v0.4 集成）
 * - 只做"时间窗口解析"和"参数校验"
 * - 实际 reproduce 由 `ReplayExecutor` 完成（v0.4 评估）
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

    public function handle(Replayer $replayer): int
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

        $this->warn('[kafka:replay] v0.3 MVP: window validated, actual reproduce not implemented yet (v0.4 评估)');

        return 0;
    }
}
