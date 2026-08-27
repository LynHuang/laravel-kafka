<?php

declare(strict_types=1);

namespace LaravelKafka\Console;

use Illuminate\Console\Command;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Message as RdKafkaMessage;

/**
 * `php artisan kafka:dlq:tail <topic>` 命令（v0.3 Step 3）。
 *
 * ## 用途
 *
 * 实时打印 DLQ topic 收到的失败消息，便于运维：
 *  - 不消费（不 commit offset），不写 DB
 *  - 打印 exception class / message / original_topic / attempts 等关键 header
 *
 * ## 与普通消费者的区别
 *
 * | 维度 | `kafka:work` | `kafka:dlq:tail` |
 * | --- | --- | --- |
 * | 业务处理 | 调 NativeHandler | 打印 + 退出（不处理） |
 * | commit offset | 整批 commit | **不 commit**（运维可重跑） |
 * | 失败处理 | DLQ / Requeue | 直接打印（DLQ 已是死信） |
 *
 * ## 业务方使用
 *
 * ```bash
 * # 实时 tail 一个 DLQ topic
 * php artisan kafka:dlq:tail laravel-jobs.dlq
 *
 * # 限制条数 + 安静模式
 * php artisan kafka:dlq:tail laravel-jobs.dlq --max=100 --quiet
 * ```
 */
final class DlqTailCommand extends Command
{
    /**
     * @var string
     */
    protected $signature = 'kafka:dlq:tail
        {topic : DLQ topic 名}
        {--connection=default : Kafka 连接名}
        {--max=0 : 最多打印条数，0 = 无限}
        {--sleep=1 : 无消息时 sleep 秒数}';

    /**
     * @var string
     */
    protected $description = '实时 tail DLQ topic（不 commit，只打印）';

    public function handle(): int
    {
        $topic = (string) $this->argument('topic');
        $connection = (string) $this->option('connection');
        $max = (int) $this->option('max');
        $sleep = max(0, (int) $this->option('sleep'));

        $config = $this->laravel->make('kafka.manager')->config($connection);

        // 独立 consumer group（避免与主消费者 / DLQ 消费者冲突）
        // v0.4.1 hotfix: 不调 $conf->get('group.id')（部分 ext-rdkafka 版本 Conf::get() 不存在），
        // 用本地变量 $groupId 保存, 然后用于打印日志.
        $groupId = 'kafka-dlq-tail-' . gethostname() . '-' . getmypid();
        $conf = new Conf();
        $conf->set('client.id', 'laravel-kafka-dlq-tail');
        $conf->set('bootstrap.servers', $config->brokers());
        $conf->set('group.id', $groupId);
        $conf->set('enable.auto.commit', 'false');
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new KafkaConsumer($conf);
        $consumer->subscribe([$topic]);

        $this->info(sprintf('[kafka:dlq:tail] tailing topic=%s group=%s', $topic, $groupId));

        $count = 0;
        while (true) {
            if ($max > 0 && $count >= $max) {
                $this->info('[kafka:dlq:tail] max reached, exiting');
                break;
            }

            $rdMsg = $consumer->consume(1000);
            if ($rdMsg === null) {
                continue;
            }

            switch ($rdMsg->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->printMessage($rdMsg);
                    $count++;
                    break;

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    if ($sleep > 0) {
                        sleep($sleep);
                    }
                    break;

                default:
                    $this->error(sprintf(
                        '[kafka:dlq:tail] error: code=%d %s',
                        $rdMsg->err,
                        rd_kafka_err2str($rdMsg->err)
                    ));
                    break;
            }
        }

        $consumer->close();
        return 0;
    }

    /**
     * 打印一条 DLQ 消息。
     */
    private function printMessage(RdKafkaMessage $rdMsg): void
    {
        $headers = [];
        if (is_array($rdMsg->headers)) {
            foreach ($rdMsg->headers as $k => $v) {
                $headers[(string) $k] = (string) $v;
            }
        }

        $lines = [
            sprintf('--- offset=%s partition=%d ---', $rdMsg->offset, $rdMsg->partition),
            'exception: ' . ($headers['x-exception-class'] ?? '?'),
            'message:   ' . ($headers['x-exception-message'] ?? '?'),
            'original:  ' . ($headers['x-original-topic'] ?? '?') . ' (partition=' . ($headers['x-original-partition'] ?? '?') . ')',
            'attempts:  ' . ($headers['x-attempts'] ?? '?'),
            'failed_at: ' . ($headers['x-failed-at'] ?? '?'),
            'queue:     ' . ($headers['x-queue'] ?? '?'),
            'connection:' . ($headers['x-connection'] ?? '?'),
            'payload:   ' . substr((string) $rdMsg->payload, 0, 200) . (strlen((string) $rdMsg->payload) > 200 ? '...' : ''),
        ];

        $this->line(implode("\n", $lines));
    }
}
