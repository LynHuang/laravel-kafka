<?php

/**
 * v0.3 Step 5: Produce 吞吐基准。
 *
 * 用法：
 *   php benchmarks/produce-throughput.php [messages=10000] [payload-size=1024] [topic=bench-prod]
 *
 * 输出：
 *   - 总耗时
 *   - 每秒消息数（msg/s）
 *   - 每秒字节数（MB/s）
 *
 * 依赖：
 *   - ext-rdkafka
 *   - 一个能连的 Kafka broker（默认 localhost:9092）
 */

require __DIR__ . '/../vendor/autoload.php';

// 参数解析
$messages = (int) ($argv[1] ?? 10000);
$payloadSize = (int) ($argv[2] ?? 1024);
$topic = (string) ($argv[3] ?? 'bench-prod');
$brokers = (string) (getenv('KAFKA_BROKERS') ?: 'localhost:9092');

echo "=== Produce Throughput Benchmark ===\n";
echo "messages:    $messages\n";
echo "payload:     $payloadSize bytes\n";
echo "topic:       $topic\n";
echo "brokers:     $brokers\n";
echo "\n";

// 构造 producer
$conf = new RdKafka\Conf();
$conf->set('bootstrap.servers', $brokers);
$conf->set('client.id', 'bench-produce');
$conf->set('enable.idempotence', 'true');
$conf->set('acks', 'all');
$conf->set('linger.ms', '5');
$conf->set('batch.size', '65536');

$producer = new RdKafka\Producer($conf);
$producerTopic = $producer->newTopic($topic);

$payload = str_repeat('x', $payloadSize);

// 发送
$startTime = microtime(true);
$startMem = memory_get_usage(true);

for ($i = 0; $i < $messages; $i++) {
    $producerTopic->producev(
        RD_KAFKA_PARTITION_UA,  // partition
        0,                       // msgflags
        $payload,                // payload
        null,                    // key
        [],                      // headers
        0,                       // timestamp
        (string) $i              // msg_opaque
    );

    // 定期 poll
    if ($i % 1000 === 0) {
        $producer->poll(0);
    }
}

// 等待所有消息投递完
$producer->flush(10000);

$endTime = microtime(true);
$endMem = memory_get_usage(true);

$duration = $endTime - $startTime;
$msgPerSec = $messages / $duration;
$bytesPerSec = ($messages * $payloadSize) / $duration / 1024 / 1024;
$memDelta = ($endMem - $startMem) / 1024 / 1024;

echo "=== Results ===\n";
printf("Duration:        %.3f s\n", $duration);
printf("Messages:        %d\n", $messages);
printf("Throughput:      %.0f msg/s\n", $msgPerSec);
printf("Bandwidth:       %.2f MB/s\n", $bytesPerSec);
printf("Memory delta:    %.2f MB\n", $memDelta);
