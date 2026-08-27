<?php

declare(strict_types=1);

namespace LaravelKafka\Console;

use Illuminate\Console\Command;
use LaravelKafka\Consumer\Subscription;
use LaravelKafka\Delay\DelayRouter;
use LaravelKafka\Producer\Message;
use LaravelKafka\Producer\Producer;
use LaravelKafka\Producer\ProducerFactory;
use LaravelKafka\Support\Header;

/**
 * `php artisan kafka:delay:work` 命令（v0.5.3 实现，v0.3 设计）。
 *
 * ## 用途
 *
 * 时间轮分层延迟消息的 worker：监听所有 tier topic（`delay-5s, delay-30s, ...`），
 * 每条消息检查 `x-available-at` header，到期后 requeue 回主 topic（`x-original-queue`）。
 *
 * ## 工作流
 *
 * ```
 * Queue::later(30s, job) → 写 "delay-30s" tier topic（带 x-available-at / x-original-queue）
 *      ↓
 * kafka:delay:work 监听所有 tier topic
 *      ↓ 到期（now >= x-available-at）
 * requeue 回主 topic（x-attempt 重置 0）+ commit 原 offset
 *      ↓
 * kafka:work 消费主 topic，业务方无感知
 * ```
 *
 * ## 到期检查
 *
 * - `now >= x-available-at` → 立即 requeue
 * - `now < x-available-at` → 同步阻塞等待剩余时间再 requeue（简单可靠，无内存状态，
 *   延迟消息场景吞吐要求低；worker 重启不丢消息——消息仍在 tier topic 里）
 *
 * ## 业务方使用
 *
 * ```bash
 * # 长驻运行（supervisor 管理）
 * php artisan kafka:delay:work
 *
 * # 调试: 跑 30s 退出
 * php artisan kafka:delay:work --max-time=30
 * ```
 *
 * @see \LaravelKafka\Delay\DelayRouter 分层路由
 * @see \LaravelKafka\Queue\KafkaQueue::later() 生产端
 */
final class DelayWorkCommand extends Command
{
    /**
     * @var string
     */
    protected $signature = 'kafka:delay:work
        {--connection=default : Kafka connection 名}
        {--group=laravel-delay-worker : 独立 consumer group (避免和 kafka:work 共享 offset)}
        {--max-time=0 : 最大运行秒数, 0 = 不限}
        {--max-jobs=0 : 最大处理任务数, 0 = 不限}
        {--sleep=1 : 无消息时的 sleep 秒数}';

    /**
     * @var string
     */
    protected $description = '启动时间轮延迟消息 worker：监听 tier topic，到期 requeue 回主 topic';

    /**
     * 信号触发后设为 true，主循环判断后退出。
     */
    private bool $shouldQuit = false;

    public function handle(ProducerFactory $producerFactory): int
    {
        $this->installSignalHandlers();

        $connection = (string) $this->option('connection');
        $config = $this->laravel->make('kafka.manager')->config($connection);
        $router = $this->laravel->make(DelayRouter::class);
        $topics = $router->allTopics();

        $subscription = new Subscription($topics);
        // v0.5.3 fix: delay:work 用独立 consumer group (默认 laravel-delay-worker),
        // 避免和 kafka:work 共享 group offset —— 共享会导致 tier 消息 offset 已被消费, poll 不到.
        $consumer = $this->buildConsumer($config, $subscription);
        $producer = $producerFactory->make($config);

        $this->info(sprintf(
            '[kafka:delay:work] starting on tier topics=[%s] group=%s',
            implode(',', $topics),
            (string) $this->option('group')
        ));

        $startTime = time();
        $jobCount = 0;
        $maxTime = (int) $this->option('max-time');
        $maxJobs = (int) $this->option('max-jobs');
        $sleep = max(0, (int) $this->option('sleep'));

        while (! $this->shouldQuit) {
            if ($maxTime > 0 && (time() - $startTime) >= $maxTime) {
                $this->info('[kafka:delay:work] max-time reached, exiting');
                break;
            }
            if ($maxJobs > 0 && $jobCount >= $maxJobs) {
                $this->info('[kafka:delay:work] max-jobs reached, exiting');
                break;
            }

            $message = null;
            try {
                $message = $consumer->poll(1000);
            } catch (\LaravelKafka\Exceptions\KafkaException $e) {
                // v0.5.3: 容忍"Unknown topic or partition" (code=3) —— tier topic 可能还没创建
                // (业务方只用了部分 tier)。librdkafka 对未创建的 topic 报 code=3,
                // worker 等待后重试, 不崩溃.
                if (strpos($e->getMessage(), 'Unknown topic') !== false
                    || strpos($e->getMessage(), 'code=3') !== false) {
                    if ($sleep > 0) {
                        sleep($sleep);
                    }
                    continue;
                }
                throw $e;
            }

            if ($message === null) {
                if ($sleep > 0) {
                    sleep($sleep);
                }
                continue;
            }

            // 到期检查: now >= x-available-at → requeue; 否则同步等待剩余时间
            $availableAt = (int) ($message->header(Header::AVAILABLE_AT) ?? 0);
            $now = (int) (microtime(true) * 1000);
            if ($availableAt > 0 && $now < $availableAt) {
                $waitMs = $availableAt - $now;
                // 分段 sleep 避免长阻塞无法响应信号
                $waitSeconds = (int) ceil($waitMs / 1000);
                // v0.5.3: $this->shouldQuit 由异步信号处理器 (pcntl_signal) 赋值,
                // phpstan 静态分析看不到, 加 ignore. 信号触发时中断等待.
                /** @phpstan-ignore-next-line */
                for ($i = 0; $i < $waitSeconds && ! $this->shouldQuit; $i++) {
                    sleep(1);
                }
            }

            /** @phpstan-ignore-next-line */
            if ($this->shouldQuit) {
                // 信号触发: 未 requeue, 不 commit, 消息留在 tier topic 下次处理
                break;
            }

            $this->requeueToMain($producer, $message);
            // commit 当前 offset (ack), 消息从 tier topic 消费位置移除
            $consumer->kafka()->commit();

            $jobCount++;
            $this->line(sprintf(
                '<info>DELAY-REQUEUE</info> tier_topic=%s -> main_topic=%s offset=%s',
                $message->header(Header::ORIGINAL_TOPIC) ?? '?',
                $message->header('x-original-queue') ?? '?',
                $message->header(Header::ORIGINAL_OFFSET) ?? '?'
            ));
        }

        $this->info('[kafka:delay:work] shutting down...');
        $consumer->close();
        $producerFactory->flushAll(5000);

        return 0;
    }

    /**
     * 内部：把到期消息 requeue 回主 topic。
     *
     * 保留原始 payload + headers（traceparent 等），覆盖：
     *  - `x-queue` = 主 topic（消费端据此还原逻辑队列）
     *  - `x-attempt` = '0'（延迟后是首次投递到主 topic）
     *  - 去掉延迟相关 header（delay_seconds / delay_tier / x-available-at，不再在 tier 里路由）
     *
     * @param Producer $producer
     * @param Message $message tier topic 的延迟消息
     * @return void
     */
    private function requeueToMain(Producer $producer, Message $message): void
    {
        $mainTopic = (string) ($message->header('x-original-queue') ?? 'laravel-jobs');
        $headers = $message->headers();

        // 覆盖 + 清理延迟相关 header
        $headers[Header::QUEUE] = $mainTopic;
        $headers[Header::RETRY_COUNT] = '0';
        unset(
            $headers['delay_seconds'],
            $headers['delay_tier'],
            $headers[Header::AVAILABLE_AT]
        );

        $producer->send($mainTopic, new Message(
            $message->payload(),
            $headers,
            $message->key()
        ));
    }

    /**
     * 内部：构造 consumer（独立 group，避免和 kafka:work 共享 offset）。
     *
     * @param \LaravelKafka\Config\KafkaConfig $config
     * @param Subscription $subscription
     * @return \LaravelKafka\Consumer\Consumer
     */
    private function buildConsumer(\LaravelKafka\Config\KafkaConfig $config, Subscription $subscription): \LaravelKafka\Consumer\Consumer
    {
        $rdConfig = $config->toConsumerRdKafkaConfig();
        // 覆盖 group.id 为独立 group
        $rdConfig['group.id'] = (string) $this->option('group');
        // auto.offset.reset 用 earliest: 延迟 worker 必须从头消费 tier topics (没消费过)
        $rdConfig['auto.offset.reset'] = 'earliest';

        $conf = new \RdKafka\Conf();
        foreach ($rdConfig as $k => $v) {
            $conf->set((string) $k, (string) $v);
        }
        $rdConsumer = new \RdKafka\KafkaConsumer($conf);
        $rdConsumer->subscribe($subscription->topics());

        return new \LaravelKafka\Consumer\Consumer($rdConsumer, $subscription);
    }

    /**
     * 内部：注册 SIGTERM / SIGINT 信号处理（Windows 无 pcntl 安全跳过）。
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
