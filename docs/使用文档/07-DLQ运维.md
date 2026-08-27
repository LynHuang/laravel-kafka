# 07 DLQ 运维

`kafka:dlq:tail` 命令 + DLQ 消息格式解读 + 故障排查流程。

---

## 1. `kafka:dlq:tail` 命令

```bash
php artisan kafka:dlq:tail <topic> [options]
```

### 位置参数

| 参数 | 说明 |
| --- | --- |
| `topic` | DLQ topic 名（必填） |

### 选项

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `--connection=default` | `default` | Kafka connection 名 |
| `--max=0` | `0`（无限） | 最多打印条数 |
| `--sleep=1` | `1` | 无消息时 sleep 秒数 |

### 行为

- 启动**独立 consumer**（不与主消费者争抢）
- 不 commit offset（运维可重跑同一批 DLQ 消息）
- 实时打印格式化的 DLQ 消息

### 输出示例

```bash
$ php artisan kafka:dlq:tail laravel-jobs.dlq
[kafka:dlq:tail] tailing topic=laravel-jobs.dlq group=kafka-dlq-tail-host123-4567
--- offset=123 partition=0 ---
exception: App\Exception\BizFatalException
message:   User not found
original:  laravel-jobs (partition=0)
attempts:  3
failed_at: 1700000000123
queue:     laravel-jobs
connection:default
payload:   O:24:"App\Jobs\SendOrderEmail":2:{s:35:"\x00App\Jobs\SendOrderEmail\x00orderId";i:12345;s:10:"\x00*\x00data";a:1:{s:4:"data";a:0:{}}}
^C
```

---

## 2. 限制条数

```bash
# 只看前 10 条
php artisan kafka:dlq:tail laravel-jobs.dlq --max=10

# 无限（默认）
php artisan kafka:dlq:tail laravel-jobs.dlq --max=0
```

看完前 10 条后 worker 自动退出。

---

## 3. DLQ 消息格式

`DlqFailedJobHandler` produce 的消息包含完整排障信息：

### Kafka headers

| header | 值示例 | 说明 |
| --- | --- | --- |
| `x-failed-at` | `1700000000123` | 失败时间（ms） |
| `x-exception-class` | `App\\Exception\\BizFatalException` | 异常类 FQN |
| `x-exception-message` | `User not found` | 异常 message（截断到 `message_truncate_bytes`） |
| `x-exception-trace` | `#0 /path/to/...` | 异常 trace（截断到 `trace_truncate_bytes`） |
| `x-original-topic` | `laravel-jobs` | 原始主 topic |
| `x-original-partition` | `0` | 原始 partition |
| `x-original-offset` | `12345` | 原始 offset（消费端 `Consumer::wrap()` 注入，经原 headers 带入） |
| `x-original-headers` | `{"x-queue":"..."}` | v0.5.2：原消息 headers 的 JSON（`DlqFailedJobHandler` 追加） |
| `x-attempts` | `3` | 重试次数（hybrid 模式时为最终 attempt） |
| `x-job-id` | `uuid` | v0.5.2：Job 唯一 id（`DlqFailedJobHandler` 追加） |
| `x-queue` | `laravel-jobs` | Laravel 逻辑队列名 |
| `x-connection` | `default` | connection 名 |
| `x-trace-id` | `abc123def456` | 追踪 ID（v0.1 兼容） |
| `traceparent` | `00-...` | W3C Trace Context（v0.2+） |

### Payload

原始消息的 payload 字符串（PHP serialize / JSON / 自定义，取决于 producer 用的 `Serializer`）。

`kafka:dlq:tail` 输出 `payload` 字段时只显示前 200 字节 + `...`（防刷屏）。要看完整 payload 用 `kcat`：

```bash
kcat -b localhost:9092 -t laravel-jobs.dlq -C -o beginning -e -q | head -1
```

或单条消费指定 offset：

```bash
kcat -b localhost:9092 -t laravel-jobs.dlq -C -p 0 -o 123 -c 1
```

---

## 4. 跨语言消费 DLQ

DLQ 是**普通 Kafka topic**，任何语言都能消费：

```python
# Python 消费 DLQ
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'laravel-jobs.dlq',
    bootstrap_servers='localhost:9092',
    group_id='dlq-analytics',
    auto_offset_reset='earliest',
)

for msg in consumer:
    exception_class = msg.headers.get('x-exception-class', [b''])[0].decode()
    failed_at = int(msg.headers.get('x-failed-at', [b'0'])[0].decode())
    print(f"[{failed_at}] {exception_class}: {msg.value[:200]}")
```

```go
// Go 消费 DLQ
reader := kafka.NewReader(kafka.ReaderConfig{
    Brokers: []string{"localhost:9092"},
    Topic:   "laravel-jobs.dlq",
    GroupID: "dlq-analyzer",
})
for {
    m, err := reader.ReadMessage(ctx)
    if err != nil { break }
    exceptionClass := string(m.Headers[0].Value)
    log.Printf("DLQ: %s @ offset=%d", exceptionClass, m.Offset)
}
```

---

## 5. 故障排查流程

### Step 1：识别异常类型

```bash
php artisan kafka:dlq:tail laravel-jobs.dlq --max=20
```

看 `exception` 字段，统计：

```bash
# 用 kcat 导出全部 DLQ，按 exception 聚合
kcat -b localhost:9092 -t laravel-jobs.dlq -C -o beginning -e -q -f '%h\n' \
    | grep -E '^x-exception-class' \
    | sort | uniq -c | sort -rn | head -10
```

### Step 2：定位异常类

- `SerializationException` → 数据反序列化失败（payload 格式变了，或 schema 不兼容）
- `ValidationException` → 业务参数校验失败
- `App\Exception\BizFatalException` → 业务自定义致命错误
- `InvalidArgumentException` → 传参错
- `RuntimeException` / `Throwable` → 通用

### Step 3：看 trace 找根因

DLQ 消息的 `x-exception-trace` header 含完整 stack trace（截断到 `trace_truncate_bytes = 32KB`）。

用 `kafka-console-consumer` 拿完整 trace：

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic laravel-jobs.dlq --max-messages 1 \
    --property print.headers=true
```

### Step 4：决定处理方式

| 异常类型 | 处理 |
| --- | --- |
| 临时性（网络超时、外部 API 503） | `queue:retry <uuid>` 重投 |
| 永久性（参数错误、bug） | 修代码 + 删 DLQ 消息（`kafka-delete-records`） |
| 业务致命（资金不足、库存为 0） | 人工审批后 `queue:retry <uuid>` 或 丢弃 |

### Step 5：批量重投

把 DLQ 消息重新 produce 到主 topic（人工修复后）：

```bash
# 写一个临时脚本批量重投
php -r '
require __DIR__."/vendor/autoload.php";
$app = require __DIR__."/bootstrap/app.php";
$kernel = $app->make(\Illuminate\Contracts\Console\Kernel::class);
$kernel->bootstrap();

$consumer = Kafka::connection()->kafka();
$consumer->subscribe(["laravel-jobs.dlq"]);
$count = 0;
while ($msg = $consumer->consume(1000)) {
    if ($msg->err !== RD_KAFKA_RESP_ERR_NO_ERROR) continue;
    $payload = $msg->payload;
    Kafka::connection()->pushRaw($payload, "laravel-jobs");
    $count++;
    if ($count >= 100) break;
}
echo "Replayed $count DLQ messages\n";
'
```

---

## 6. DLQ 容量规划

### retention

```dotenv
KAFKA_DLQ_RETENTION_MS=1209600000  # 14 天
```

14 天后 broker 自动删。业务方按合规要求调整（金融行业可能要求 90 天）。

### 估算

```
DLQ 容量 = 失败率 × 业务消息量 × 平均消息大小 × retention_ms / 时间窗口
```

例：每秒 1000 条业务消息，失败率 0.1%，平均 4KB，retention 14 天：
- 每天失败 = 1000 × 86400 × 0.001 = 86,400 条
- 每天容量 = 86,400 × 4KB ≈ 345 MB
- 14 天 = 4.8 GB

建议给 DLQ topic 单独配额 + 监控告警（接近 quota 时报警）。

---

## 7. 监控告警

业务方应监控 DLQ 写入速率：

```bash
# 用 kafka-run-class.sh 算 DLQ 写入速率
kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic laravel-jobs.dlq \
    --time -1
```

或用 JMX metrics：`kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=laravel-jobs.dlq`。

业务方常用监控：Prometheus + kafka_exporter + Grafana。

---

## 8. 完整示例：自动化清理过期 DLQ

```php
// app/Console/Commands/PruneOldDlq.php
namespace App\Console\Commands;

use Illuminate\Console\Command;

class PruneOldDlq extends Command
{
    protected $signature = 'kafka:dlq:prune {--days=14 : 删除 N 天前的 DLQ 消息}';
    protected $description = '删除指定天数前的 DLQ 消息';

    public function handle(): int
    {
        $days = (int) $this->option('days');
        $cutoffMs = (int) (microtime(true) * 1000) - $days * 86400 * 1000;

        // 用 librdkafka 删除到 cutoff offset
        // v0.5.2 修正: KafkaConfig 没有 kafka() 方法. queryWatermarkOffsets 在
        // RdKafka\Consumer 上 (见 KafkaQueue::size() 的用法). 这里用独立 Consumer:
        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', Kafka::config('default')->brokers());
        $meta = new \RdKafka\Consumer($conf);
        $low = 0;
        $high = 0;
        $meta->queryWatermarkOffsets('laravel-jobs.dlq', 0, $low, $high, 5000);
        // 业务方根据 cutoffMs 计算要删的 offset，调 deleteRecords API
        $this->info("Pruned DLQ messages older than $days days");
        return 0;
    }
}
```

加到 scheduler：

```php
// app/Console/Kernel.php
protected function schedule(Schedule $schedule)
{
    $schedule->command('kafka:dlq:prune --days=14')->daily();
}
```

---

## 下一步

- 回溯 Replay：[08-回溯Replay](08-回溯Replay.md)
- Horizon 适配：[09-Horizon适配](09-Horizon适配.md)
- CLI 命令：[15-CLI命令清单](15-CLI命令清单.md)
