# 15 CLI 命令清单

`laravel-kafka` 注册 4 个 Artisan 命令。详细使用见各专题章节。

---

## 命令总览

| 命令 | 用途 | 章节 |
| --- | --- | --- |
| `kafka:work` | 启动消费者 worker 长进程 | [04-Consumer-消费消息](04-Consumer-消费消息.md) |
| `kafka:dlq:tail` | 实时打印 DLQ 消息 | [07-DLQ运维](07-DLQ运维.md) |
| `kafka:replay` | 时间窗口重放 | [08-回溯Replay](08-回溯Replay.md) |
| `kafka:horizon:snapshot` | 把 metrics 快照到 Redis | [09-Horizon适配](09-Horizon适配.md) |

> v0.5.3 起：`kafka:delay:work`（时间轮延迟 worker）已发布，见 [06-延迟消息 §5](06-延迟消息.md#5-kafkadelayworkv053-实现)。

---

## 1. `kafka:work`

启动 Kafka 消费者 worker 长进程。

```bash
php artisan kafka:work [options]
```

### 选项

| 选项 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `--queue=*` | string[] | `default` topic | 订阅的 topic 列表，**可多次指定**或**逗号分隔** |
| `--connection=default` | string | `default` | Kafka connection 名 |
| `--max-time=0` | int | `0`（不限） | 最大运行秒数，到时优雅退出 |
| `--max-jobs=0` | int | `0`（不限） | 最大处理任务数，到时优雅退出 |
| `--sleep=1` | int | `1` | 无消息时 sleep 秒数 |
| `--batch-size=1` | int | `1` | 批量消费：单次 `pollBatch` 最多拉取消息数 |
| `--batch-timeout=2000` | int | `2000` | 批量消费：单次 `pollBatch` 总超时（ms） |
| `--horizon-metrics` | bool | `false` | 启用 Horizon 兼容 metrics |
| `--horizon-prefix=horizon:` | string | `horizon:` | Horizon Redis key 前缀 |
| `--horizon-redis=horizon` | string | `horizon` | Horizon Redis connection 名 |

### 示例

```bash
# 单 topic
php artisan kafka:work --queue=laravel-jobs

# 多 topic（逗号）
php artisan kafka:work --queue=emails,orders,reports

# 多 topic（多次指定）
php artisan kafka:work --queue=emails --queue=orders

# 限制运行
php artisan kafka:work --max-time=3600 --max-jobs=10000

# 批量消费
php artisan kafka:work --batch-size=50 --batch-timeout=3000

# 带 Horizon metrics
php artisan kafka:work --horizon-metrics

# 多 connection
php artisan kafka:work --connection=reports --queue=order-events
```

### 输出示例

```
[kafka:work] starting on topics=[laravel-jobs] group=laravel-default
[kafka:work] batch mode: max=50 timeout=3000ms
ACK offset=0 topic=laravel-jobs
ACK offset=1 topic=laravel-jobs
REQUEUE offset=2 topic=laravel-jobs attempt=1
ACK offset=3 topic=laravel-jobs
DLQ offset=4 topic=laravel-jobs err=App\Exception\BizFatalException
^C
[kafka:work] shutting down...
```

### 优雅退出

- Linux/macOS：`kill -TERM <pid>` / `Ctrl+C` (SIGINT)
- Windows：`Ctrl+C`（无 pcntl_signal，跳过信号处理但 worker 仍会 flush 退出）

### supervisor 配置

```ini
[program:laravel-kafka-work]
command=php /var/www/laravel/artisan kafka:work --max-time=3600 --sleep=2
autostart=true
autorestart=true
user=www-data
numprocs=4
redirect_stderr=true
stdout_logfile=/var/log/laravel-kafka.log
stopwaitsecs=30
```

---

## 2. `kafka:dlq:tail`

实时打印 DLQ 消息（不 commit）。

```bash
php artisan kafka:dlq:tail <topic> [options]
```

### 位置参数

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `topic` | ✅ | DLQ topic 名 |

### 选项

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `--connection=default` | `default` | Kafka connection 名 |
| `--max=0` | `0`（无限） | 最多打印条数 |
| `--sleep=1` | `1` | 无消息时 sleep 秒数 |

### 示例

```bash
# 无限 tail
php artisan kafka:dlq:tail laravel-jobs.dlq

# 只看 10 条
php artisan kafka:dlq:tail laravel-jobs.dlq --max=10

# 多 connection 的 DLQ
php artisan kafka:dlq:tail reports.dlq --connection=reports
```

### 输出示例

```
[kafka:dlq:tail] tailing topic=laravel-jobs.dlq group=kafka-dlq-tail-host123-4567
--- offset=123 partition=0 ---
exception: App\Exception\BizFatalException
message:   User not found
original:  laravel-jobs (partition=0)
attempts:  3
failed_at: 1700000000123
queue:     laravel-jobs
connection:default
payload:   O:24:"App\Jobs\SendOrderEmail":2:{...}
--- offset=124 partition=0 ---
exception: InvalidArgumentException
message:   Amount must be > 0
...
^C
```

### 行为

- 独立 consumer group（`kafka-dlq-tail-<hostname>-<pid>`），不与主消费者争抢
- 不 commit offset，运维可重跑同一批
- `Ctrl+C` 退出

---

## 3. `kafka:replay`

按时间窗口重放 topic。

```bash
php artisan kafka:replay [options]
```

### 选项

| 选项 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `--topic=` | ✅ | — | 源 topic |
| `--from=` | ✅ | — | 起始时间（见 [08-回溯Replay §3](08-回溯Replay.md#3-时间窗口格式)） |
| `--to=` | ✅ | — | 结束时间 |
| `--target-topic=` | ✅ | — | 目标 topic |
| `--group=replay-runner` | ❌ | `replay-runner` | 独立 consumer group |

### 时间格式

| 输入 | 含义 |
| --- | --- |
| `now` | 当前时间 |
| `-1h` / `-30m` / `-7d` / `-60s` | 相对时间 |
| `1700000000` | 绝对 Unix timestamp |
| `2026-08-25 10:00:00` | 绝对时间字符串（`strtotime`） |

### 示例

```bash
# 过去 1 小时 → 重放到 replay topic
php artisan kafka:replay \
    --topic=orders.events \
    --from="-1h" \
    --to=now \
    --target-topic=orders.events.replay

# 绝对时间窗口
php artisan kafka:replay \
    --topic=orders.events \
    --from="2026-08-25 10:00:00" \
    --to="2026-08-25 11:00:00" \
    --target-topic=orders.events.replay

# 独立 group
php artisan kafka:replay \
    --topic=orders.events \
    --from="-30m" \
    --to=now \
    --target-topic=orders.events.replay \
    --group=my-replay-runner
```

### 输出

```
[kafka:replay] topic=orders.events window=[-1h (1700000000) → now (1700003600)] target=orders.events.replay group=replay-runner
[kafka:replay] done: replayed 128 message(s) from 3 partition(s) to "orders.events.replay"
```

### v0.5.3 实现

> v0.5.3 起**实际 reproduce**（`offsetsForTimes` + 遍历 partition 重放，见 [08-回溯Replay §2](08-回溯Replay.md#2-v053-实际-reproduce)）。
> v0.3 MVP 只做窗口解析，v0.5.3 补全重放。

---

## 4. `kafka:horizon:snapshot`

把 Kafka 队列 metrics 快照到 Redis（Horizon 兼容格式）。

```bash
php artisan kafka:horizon:snapshot [options]
```

### 选项

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `--connection=horizon` | `horizon` | Redis connection 名 |
| `--prefix=horizon:` | `horizon:` | Redis key 前缀 |
| `--trim=24` | `24` | 每个 queue 保留的快照数 |
| `--trim-job=24` | `24` | 每个 job 保留的快照数 |

### 示例

```bash
php artisan kafka:horizon:snapshot
php artisan kafka:horizon:snapshot --trim=48 --trim-job=48
```

### 输出

```
[kafka:horizon:snapshot] v0.4.4: 真跑 snapshot (v0.4.0-0.4.3 是 stub).
[kafka:horizon:snapshot] snapshotted 1 queue(s), 1 job(s) to prefix="horizon:" connection="horizon"
业务方应同时启用 Horizon 自身 snapshot 任务（如果已装 Horizon）。
```

> **v0.5.2 修正**：v0.4.4+ 已**真跑 snapshot**（写 `snapshot:queue:` / `snapshot:job:` zset，
> v0.4.6 修 job 路径），不再是旧 stub。

### 加到 scheduler

```php
// app/Console/Kernel.php
protected function schedule(Schedule $schedule)
{
    // 推荐：业务方已装 Horizon 时，用 Horizon 自带命令
    $schedule->command('horizon:snapshot')->everyMinute();

    // 或用本包命令（不依赖 Horizon）
    $schedule->command('kafka:horizon:snapshot')->everyMinute();
}
```

---

## 5. 命令列表速查

```bash
php artisan list | grep kafka
# kafka:dlq:tail
# kafka:horizon:snapshot
# kafka:replay
# kafka:work
```

---

## 6. 命令依赖

| 命令 | ext-rdkafka | Kafka broker | Redis（仅 `--horizon-metrics`） |
| --- | :-: | :-: | :-: |
| `kafka:work` | ✅ | ✅ | 可选 |
| `kafka:dlq:tail` | ✅ | ✅ | ❌ |
| `kafka:replay` | ✅ | ✅ | ❌ |
| `kafka:horizon:snapshot` | ❌ | ❌ | ✅ |

> 业务方任何命令缺依赖时 Artisan 会报"Class not found"或"Fatal error"。

---

## 7. 调试技巧

### 启用 librdkafka debug

```bash
KAFKA_DEBUG=all php artisan kafka:work --queue=laravel-jobs
```

或在 `config/kafka.php` 加：

```php
'producer' => [
    'debug' => 'broker,topic,msg',
    // ...
],
```

### 看完整 producer / consumer 配置

```php
$config = Kafka::config('default');
$rdConfig = $config->toProducerRdKafkaConfig();
print_r($rdConfig);
```

### 命令行手动 produce 测试

```bash
# 用 kcat 写一条消息
echo "hello kafka" | kcat -P -b localhost:9092 -t test-topic

# 启动 worker 消费
php artisan kafka:work --queue=test-topic
```

---

## 下一步

- 高级主题：[16-高级主题](16-高级主题.md)
