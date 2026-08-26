<?php

/**
 * v0.3 Step 5: 端到端延迟分布基准。
 *
 * 用法：
 *   php benchmarks/latency-p50-p99.php [samples=1000] [topic=bench-latency]
 *
 * 测 produce → consume 完整链路的延迟分布。
 * 不追求高吞吐（用 produce() 单条同步等 ack），重点在延迟。
 *
 * 依赖：
 *   - ext-rdkafka
 *   - Kafka broker
 */

require __DIR__ . '/../vendor/autoload.php';

$samples = (int) ($argv[1] ?? 1000);
$topic = (string) ($argv[2] ?? 'bench-latency');
$brokers = (string) (getenv('KAFKA_BROKERS') ?: 'localhost:9092');

echo "=== End-to-End Latency Benchmark ===\n";
echo "samples:  $samples\n";
echo "topic:    $topic\n";
echo "brokers:  $brokers\n";
echo "\n";

// Producer
$pConf = new RdKafka\Conf();
$pConf->set('bootstrap.servers', $brokers);
$pConf->set('enable.idempotence', 'true');
$pConf->set('acks', '1');  // acks=1 测真实延迟（acks=all 更慢）
$producer = new RdKafka\Producer($pConf);
$producerTopic = $producer->newTopic($topic);

// Consumer
$cConf = new RdKafka\Conf();
$cConf->set('bootstrap.servers', $brokers);
$cConf->set('group.id', 'bench-latency-' . getmypid());
$cConf->set('enable.auto.commit', 'false');
$cConf->set('auto.offset.reset', 'earliest');
$consumer = new RdKafka\KafkaConsumer($cConf);
$consumer->subscribe([$topic]);

// 等 consumer 准备好
sleep(2);

// 测 N 轮 produce → consume
$latencies = [];
for ($i = 0; $i < $samples; $i++) {
    $startMs = microtime(true) * 1000;

    // produce（带时间戳 header 方便对照）
    $producerTopic->producev(
        RD_KAFKA_PARTITION_UA,
        0,
        "msg-$i",
        null,
        ['start_ms' => (string) $startMs],
        0,
        null
    );
    $producer->poll(0);

    // consume 一条
    $deadline = microtime(true) + 2;  // 最多等 2s
    while (microtime(true) < $deadline) {
        $msg = $consumer->consume(100);
        if ($msg === null) {
            continue;
        }
        if ($msg->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
            $endMs = microtime(true) * 1000;
            $latencies[] = $endMs - $startMs;
            break;
        }
    }
}

$consumer->close();
$producer->flush(5000);

if (empty($latencies)) {
    echo "No samples collected (check Kafka connection).\n";
    exit(1);
}

sort($latencies);
$count = count($latencies);
$p50 = $latencies[(int) ($count * 0.5)];
$p90 = $latencies[(int) ($count * 0.9)];
$p95 = $latencies[(int) ($count * 0.95)];
$p99 = $latencies[(int) ($count * 0.99)];
$min = $latencies[0];
$max = end($latencies);
$avg = array_sum($latencies) / $count;

echo "=== Results ===\n";
printf("Samples:   %d / %d\n", $count, $samples);
printf("Latency (ms):\n");
printf("  min:     %.2f\n", $min);
printf("  avg:     %.2f\n", $avg);
printf("  p50:     %.2f\n", $p50);
printf("  p90:     %.2f\n", $p90);
printf("  p95:     %.2f\n", $p95);
printf("  p99:     %.2f\n", $p99);
printf("  max:     %.2f\n", $max);
