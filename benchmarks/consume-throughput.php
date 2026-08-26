<?php

/**
 * v0.3 Step 5: Consume 吞吐基准。
 *
 * 用法：
 *   php benchmarks/consume-throughput.php [messages=10000] [topic=bench-prod] [timeout=30]
 *
 * 输出：
 *   - 总耗时
 *   - 每秒消息数（msg/s）
 *   - 端到端延迟均值
 *
 * 依赖：
 *   - ext-rdkafka
 *   - 一个能连的 Kafka broker（默认 localhost:9092）
 *   - 目标 topic 必须有数据（先用 produce-throughput.php 灌入）
 */

require __DIR__ . '/../vendor/autoload.php';

// 参数解析
$expectedMessages = (int) ($argv[1] ?? 10000);
$topic = (string) ($argv[2] ?? 'bench-prod');
$timeout = (int) ($argv[3] ?? 30);
$brokers = (string) (getenv('KAFKA_BROKERS') ?: 'localhost:9092');

echo "=== Consume Throughput Benchmark ===\n";
echo "expected:   $expectedMessages\n";
echo "topic:      $topic\n";
echo "timeout:    $timeout s\n";
echo "brokers:    $brokers\n";
echo "\n";

// 构造 consumer
$conf = new RdKafka\Conf();
$conf->set('bootstrap.servers', $brokers);
$conf->set('client.id', 'bench-consume');
$conf->set('group.id', 'bench-consume-' . getmypid());
$conf->set('enable.auto.commit', 'false');
$conf->set('auto.offset.reset', 'earliest');

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe([$topic]);

// 消费直到拉够 $expectedMessages 条或超时
$startTime = microtime(true);
$endTime = $startTime + $timeout;
$count = 0;
$latencies = [];

while ($count < $expectedMessages && microtime(true) < $endTime) {
    $msg = $consumer->consume(1000);
    if ($msg === null) {
        continue;
    }

    if ($msg->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
        $count++;
        // 计算 broker → consumer 端到端延迟
        if ($msg->timestamp > 0) {
            $latency = (microtime(true) * 1000) - $msg->timestamp;
            $latencies[] = $latency;
        }
    } elseif ($msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF
        || $msg->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
        continue;
    } else {
        echo "Error: " . $msg->errstr() . "\n";
    }
}

// 整批 commit
$consumer->commitAsync();
$consumer->close();

$duration = microtime(true) - $startTime;
$msgPerSec = $count / $duration;

echo "=== Results ===\n";
printf("Duration:    %.3f s\n", $duration);
printf("Consumed:    %d / %d\n", $count, $expectedMessages);
printf("Throughput:  %.0f msg/s\n", $msgPerSec);

if (! empty($latencies)) {
    sort($latencies);
    $p50 = $latencies[(int) (count($latencies) * 0.5)];
    $p95 = $latencies[(int) (count($latencies) * 0.95)];
    $p99 = $latencies[(int) (count($latencies) * 0.99)];
    $max = end($latencies);

    echo "\n--- End-to-end latency (ms) ---\n";
    printf("p50:  %.2f\n", $p50);
    printf("p95:  %.2f\n", $p95);
    printf("p99:  %.2f\n", $p99);
    printf("max:  %.2f\n", $max);
}
