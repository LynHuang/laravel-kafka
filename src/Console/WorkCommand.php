<?php

declare(strict_types=1);

namespace LaravelKafka\Console;

use Illuminate\Console\Command;
use LaravelKafka\Consumer\Consumer;
use LaravelKafka\Consumer\Handler\HandlerResolver;
use LaravelKafka\Consumer\Subscription;
use LaravelKafka\Producer\ProducerFactory;

/**
 * `php artisan kafka:work` 长驻 worker 命令（v0.1）。
 *
 * ## 设计
 *
 * - **单进程**长驻消费（v0.3 评估多进程）
 * - **短轮询** 1s（`Consumer::poll(1000)`）
 * - **SIGTERM / SIGINT** 优雅退出（处理完当前 message 后 commit offset 再退出）
 * - **max-time / max-jobs** 上限（用 supervisor / systemd 时避免 worker 跑太久）
 *
 * ## 命令签名
 *
 * ```bash
 * php artisan kafka:work
 *   --queue=emails,orders          # 订阅 topics
 *   --connection=default          # Kafka 连接名
 *   --max-time=3600               # 最大运行秒数（0 = 不限）
 *   --max-jobs=1000               # 最大处理任务数（0 = 不限）
 *   --sleep=1                     # 无消息时 sleep 秒数
 * ```
 *
 * ## 优雅退出
 *
 * 用 `pcntl_signal` 监听 SIGTERM / SIGINT，`pcntl_async_signals(true)` 启用异步信号。
 * 信号触发时设 `$shouldQuit = true`，主循环下次判断时退出。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 用 `kafka:consume` + 一次性消费完退出；
 * 我们用 `kafka:work` + 长驻循环 + 优雅退出（更接近 Laravel `queue:work` 语义）。
 */
final class WorkCommand extends Command
{
    /**
     * @var string
     */
    protected $signature = 'kafka:work
        {--queue=* : 订阅的 topic 列表（可多次指定，逗号分隔）}
        {--connection=default : Kafka 连接名}
        {--max-time=0 : 最大运行秒数，0 = 不限}
        {--max-jobs=0 : 最大处理任务数，0 = 不限}
        {--sleep=1 : 无消息时的 sleep 秒数}
        {--batch-size=1 : 批量消费（v0.3）：每次 poll 最多拉取消息数，1 = 单条行为（v0.1/v0.2 默认）}
        {--batch-timeout=2000 : 批量消费（v0.3）：单次 pollBatch 总超时（ms），到时即返回已拉到的消息}';

    /**
     * @var string
     */
    protected $description = '启动 Kafka 消费者 worker 长进程';

    /**
     * 信号触发后设为 true，主循环判断后退出。
     */
    private bool $shouldQuit = false;

    /**
     * 主入口（`php artisan kafka:work` 调用）。
     *
     * ## 流程
     *
     *  1. `installSignalHandlers()` 注册 SIGTERM / SIGINT 处理
     *  2. 从 `--connection` 拿 KafkaConfig
     *  3. `parseTopics()` 解析 `--queue`（多次指定 + 逗号分隔）
     *  4. 构造 `Subscription` + 通过 `ConsumerFactory::make()` 拿 consumer
     *  5. 主循环：poll → process → sleep → 退出条件检查
     *  6. 退出：`consumer->close()` + `producerFactory->flushAll(5000)`
     *
     * @param HandlerResolver $resolver handler 路由（v0.1 全走 NativeHandler）
     * @param ProducerFactory $producerFactory 退出时 flush 所有 producer
     * @return int 0 = 正常退出
     */
    public function handle(HandlerResolver $resolver, ProducerFactory $producerFactory): int
    {
        $this->installSignalHandlers();

        $config = $this->laravel->make('kafka.manager')->config(
            (string) $this->option('connection')
        );
        $queueOption = (array) $this->option('queue');
        $topics = $this->parseTopics($queueOption, $config->defaultTopic());

        $subscription = new Subscription($topics);
        $consumer = $this->laravel->make(\LaravelKafka\Consumer\ConsumerFactory::class)
            ->make($config, $subscription);

        $this->info(sprintf(
            '[kafka:work] starting on topics=[%s] group=%s',
            implode(',', $topics),
            (string) $config->consumer()['group_id']
        ));

        $startTime = time();
        $jobCount = 0;
        $maxTime = (int) $this->option('max-time');
        $maxJobs = (int) $this->option('max-jobs');
        $sleep = max(0, (int) $this->option('sleep'));
        $batchSize = max(1, (int) $this->option('batch-size'));
        $batchTimeout = max(100, (int) $this->option('batch-timeout'));

        if ($batchSize > 1) {
            $this->info(sprintf('[kafka:work] batch mode: max=%d timeout=%dms', $batchSize, $batchTimeout));
        }

        while (! $this->shouldQuit) {
            if ($maxTime > 0 && (time() - $startTime) >= $maxTime) {
                $this->info('[kafka:work] max-time reached, exiting');
                break;
            }
            if ($maxJobs > 0 && $jobCount >= $maxJobs) {
                $this->info('[kafka:work] max-jobs reached, exiting');
                break;
            }

            // v0.3 批量消费分支
            if ($batchSize > 1) {
                $messages = $consumer->pollBatch($batchSize, $batchTimeout);
                if (empty($messages)) {
                    if ($sleep > 0) {
                        sleep($sleep);
                    }
                    continue;
                }

                // 整批处理：单条失败抛异常 → 不 commit → 整批重投
                $batchSuccess = true;
                foreach ($messages as $message) {
                    try {
                        $this->processMessage($resolver, $consumer, $message);
                    } catch (\Throwable $e) {
                        $this->error(sprintf(
                            '[kafka:work] batch message failed: %s — not committing batch',
                            $e->getMessage()
                        ));
                        $batchSuccess = false;
                        break;
                    }
                }

                if ($batchSuccess) {
                    $consumer->commitBatch();
                }
                $jobCount += count($messages);
                continue;
            }

            // v0.1/v0.2 单条行为（--batch-size=1 或默认）
            $message = $consumer->poll(1000);
            if ($message === null) {
                if ($sleep > 0) {
                    sleep($sleep);
                }
                continue;
            }

            $this->processMessage($resolver, $consumer, $message);
            $jobCount++;
        }

        $this->info('[kafka:work] shutting down...');
        $consumer->close();
        $producerFactory->flushAll(5000);

        return 0;
    }

    /**
     * 处理单条消息（调 handler + 打印结果）。
     *
     * ## 流程
     *
     *  1. `resolver->resolve($topic, $message)` 拿 handler（v0.1 全是 NativeHandler）
     *  2. `$handler->handle($message)` 拿 `HandlerResult`
     *  3. 按 action 分类打印日志（ack / requeue / dlq）
     *
     * 真正的 ack / requeue / DLQ 逻辑在 {@see \LaravelKafka\Consumer\Handler\NativeHandler} 里完成，
     * 本方法**只**负责路由 + 日志。
     *
     * @param HandlerResolver $resolver
     * @param Consumer $consumer
     * @param \LaravelKafka\Producer\Message $message 来自 consumer->poll()
     * @return void
     */
    private function processMessage(
        HandlerResolver $resolver,
        Consumer $consumer,
        \LaravelKafka\Producer\Message $message
    ): void {
        $topic = $message->header('x-original-topic') ?? 'laravel-jobs';

        $handler = $resolver->resolve($topic, $message);
        $result = $handler->handle($message);

        switch ($result->action()) {
            case \LaravelKafka\Consumer\Handler\HandlerResult::ACTION_ACK:
                // ack 已经在 KafkaJob::delete() 里完成了
                $this->line(sprintf(
                    '<info>ACK</info> offset=%s topic=%s',
                    $message->header('x-original-offset') ?? '?',
                    $topic
                ));
                break;

            case \LaravelKafka\Consumer\Handler\HandlerResult::ACTION_REQUEUE:
                // requeue 已经在 NativeHandler 里通过 producer 完成了
                $this->warn(sprintf(
                    'REQUEUE offset=%s topic=%s attempt=%s',
                    $message->header('x-original-offset') ?? '?',
                    $topic,
                    $message->header('x-attempt') ?? '?'
                ));
                break;

            case \LaravelKafka\Consumer\Handler\HandlerResult::ACTION_DLQ:
                // DLQ 已经在 failedHandler 里完成了
                $error = $result->error();
                $this->error(sprintf(
                    'DLQ offset=%s topic=%s err=%s',
                    $message->header('x-original-offset') ?? '?',
                    $topic,
                    $error !== null ? get_class($error) : 'unknown'
                ));
                break;
        }
    }

    /**
     * 解析 `--queue` 参数。
     *
     * 支持：
     *  - 多次指定：`--queue=emails --queue=orders`
     *  - 逗号分隔：`--queue=emails,orders`
     *  - 混合：去重 + trim + 跳过空
     *  - 空时 fallback 到 `defaultTopic`
     *
     * @param array<int,string> $queueOption `--queue` 的原始值（多次指定）
     * @param string $defaultTopic KafkaConfig 的默认 topic
     * @return array<int,string> 去重后的 topics
     */
    private function parseTopics(array $queueOption, string $defaultTopic): array
    {
        $topics = [];
        foreach ($queueOption as $opt) {
            foreach (explode(',', (string) $opt) as $t) {
                $t = trim($t);
                if ($t !== '') {
                    $topics[] = $t;
                }
            }
        }
        if (count($topics) === 0) {
            $topics[] = $defaultTopic;
        }
        return array_values(array_unique($topics));
    }

    /**
     * 注册 SIGTERM / SIGINT 信号处理（异步信号 + 设 `$shouldQuit`）。
     *
     * Windows 环境**没有** `pcntl_signal` 函数，安全跳过。
     *
     * @return void
     */
    private function installSignalHandlers(): void
    {
        if (! function_exists('pcntl_signal')) {
            return;
        }
        pcntl_async_signals(true);
        $handler = function (): void {
            $this->shouldQuit = true;
        };
        pcntl_signal(SIGTERM, $handler);
        pcntl_signal(SIGINT, $handler);
    }
}
