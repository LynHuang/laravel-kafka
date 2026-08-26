# 04 Consumer — 消费消息

本章涵盖 `kafka:work` worker 命令的所有选项、批量消费、优雅退出，以及直接用低层 `Consumer` API。

---

## 1. `kafka:work` 命令

```bash
php artisan kafka:work [options]
```

### 选项

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `--queue=*` | `default` topic | 订阅的 topic 列表，**可多次指定**或**逗号分隔** |
| `--connection=default` | `default` | Kafka connection 名 |
| `--max-time=0` | `0`（不限） | worker 最大运行秒数，到时优雅退出 |
| `--max-jobs=0` | `0`（不限） | worker 最大处理任务数，到时优雅退出 |
| `--sleep=1` | `1` | 无消息时 sleep 秒数 |
| `--batch-size=1` | `1` | 批量消费：单次 `pollBatch` 最多拉取消息数（`1` = 单条） |
| `--batch-timeout=2000` | `2000` | 批量消费：单次 `pollBatch` 总超时（ms） |
| `--horizon-metrics` | `false` | 启用 Horizon 兼容 metrics |
| `--horizon-prefix=horizon:` | `horizon:` | Horizon Redis key 前缀 |
| `--horizon-redis=horizon` | `horizon` | Horizon Redis connection 名 |

### 订阅 topics — 3 种写法

```bash
# 方式 1：单 topic
php artisan kafka:work --queue=laravel-jobs

# 方式 2：逗号分隔
php artisan kafka:work --queue=emails,orders,reports

# 方式 3：多次指定
php artisan kafka:work --queue=emails --queue=orders --queue=reports
```

不指定 `--queue` 时 fallback 到 `config('kafka.connections.*.queue')` 默认 topic。

### 典型部署（supervisor / systemd）

```ini
; /etc/supervisor/conf.d/laravel-kafka.conf
[program:laravel-kafka-work]
process_name=%(program_name)s_%(process_num)02d
command=php /var/www/laravel/artisan kafka:work --max-time=3600 --sleep=2
autostart=true
autorestart=true
stopasgroup=true
killasgroup=true
user=www-data
numprocs=4
redirect_stderr=true
stdout_logfile=/var/log/laravel-kafka.log
stopwaitsecs=30
```

> `numprocs=4` = 4 个 worker 进程，**同一个** `KAFKA_GROUP_ID`，kafka 自动按 partition 分配，每个 partition 只有一个 worker 消费。

---

## 2. 批量消费

```bash
php artisan kafka:work --queue=laravel-jobs --batch-size=50 --batch-timeout=2000
```

### 行为

- 每次 `pollBatch(50, 2000)`：
  - 拉到 50 条 → 立即返回
  - 或 累计耗时达 2000ms → 返回已拉到的
  - 或拉至少 1 条后遇到 TIMED_OUT → 早返回
  - 或连续 2 次 TIMED_OUT（broker 立即说无消息）→ 退出
- 整批处理：**单条失败 → 整批不 commit → 整批下次重投**

### 适用场景

- 高吞吐：批量 commit 减少 broker RPC
- 副作用合并：每批 1 次 DB 写入而非 50 次
- 顺序保证：整批原子（all-or-nothing）

### 注意事项

- 批内单条失败**不**区分"哪条失败"——整批重投时 broker 会重新投递**整批**
- 业务方需保证 Job 内部幂等（用 `x-attempt` header 判断）
- 极端情况下（同批有 1 条毒消息）会无限重投 → 业务方用 `KAFKA_MAX_ATTEMPTS` 控制

---

## 3. 优雅退出

### Linux / macOS

```bash
kill -TERM <pid>   # 触发 SIGTERM
# 或
kill -INT <pid>    # 触发 SIGINT (Ctrl+C)
```

worker 行为：

1. 当前消息处理完才退出（不打断）
2. ack / commit 当前消息
3. flush 所有 in-flight producer
4. 关闭 consumer（释放 librdkafka fd）
5. 退出码 0

### Windows

`pcntl_signal` 不存在，跳过信号处理。直接 Ctrl+C 强杀。

### `max-time` / `max-jobs` 自退出

```bash
# 跑满 1 小时自动退出（supervisor 拉起新进程接管）
php artisan kafka:work --max-time=3600

# 处理满 1000 条任务自动退出
php artisan kafka:work --max-jobs=1000
```

适合"worker 进程定期回收"场景，避免 librdkafka 长期运行的内存碎片。

---

## 4. 自定义 Handler（v0.1 占位，v0.3 增强）

默认所有消息都走 `LaravelKafka\Consumer\Handler\NativeHandler`（按 Laravel `Job` 实例化 + 调 `handle()`）。

### 注册自定义 Handler

`LaravelKafkaServiceProvider` 启动时调用 `HandlerResolver::resolve($topic, $message)` 决定用哪个 handler，业务方可重写。

```php
namespace App\Kafka\Handlers;

use LaravelKafka\Consumer\Handler\HandlerInterface;
use LaravelKafka\Consumer\Handler\HandlerResult;
use LaravelKafka\Producer\Message;

class OrderEventHandler implements HandlerInterface
{
    public function handle(Message $message): HandlerResult
    {
        $payload = json_decode($message->payload(), true);
        \Log::info('Order event', $payload);

        // 返回 ack 表示成功
        return HandlerResult::ack();
    }
}
```

绑定到容器（v0.3+）：

```php
// app/Providers/AppServiceProvider.php
public function register(): void
{
    $this->app->tag([OrderEventHandler::class], 'kafka.handlers');
}
```

> 注：v0.4.1 当前版本 per-topic handler 是占位，v0.5 完善为 `HandlerResolver` 支持数组路由。

### HandlerResult 三种 action

| action | 触发 | 副作用 |
| --- | --- | --- |
| `ACK` | `HandlerResult::ack()` | commit offset，worker 打印 `<info>ACK</info>` |
| `REQUEUE` | `HandlerResult::requeue()` | 通过 producer 重新发回主 topic（`x-attempt + 1`），worker 打印 `<comment>REQUEUE</comment>` |
| `DLQ` | `HandlerResult::dlq($error)` | 走 `FailedHandler`（database / dlq / hybrid），worker 打印 `<error>DLQ</error>` |

---

## 5. 直接用低层 `Consumer` API

业务方有自定义 poll 循环需求时：

```php
use LaravelKafka\Consumer\Consumer;
use LaravelKafka\Consumer\ConsumerFactory;

$factory = app(ConsumerFactory::class);
$config  = Kafka::config('default');
$subscription = new \LaravelKafka\Consumer\Subscription(['laravel-jobs']);
$consumer = $factory->make($config, $subscription);

// 单条 poll
while (true) {
    $message = $consumer->poll(1000);  // 1s 超时
    if ($message === null) {
        continue;  // 超时
    }

    // 处理 $message
    // 成功 / 失败由业务方自己决定 ack 还是 requeue
    $consumer->ack($consumer->kafka()->consume(0));  // 复杂场景
}

// 批量 poll
$messages = $consumer->pollBatch(50, 2000);
foreach ($messages as $message) {
    // 处理
}
$consumer->commitBatch();

// 进程退出
$consumer->close();
```

### Consumer API 完整列表

| 方法 | 说明 |
| --- | --- |
| `poll(int $timeoutMs = 1000): ?Message` | 拉 1 条消息，超时返回 `null` |
| `pollBatch(int $max, int $timeoutMs = 1000): array` | 批量拉，最多 `max` 条 / 总超时 `timeoutMs` |
| `ack(\RdKafka\Message $rdMessage): void` | 同步 commit 单条 offset |
| `commitAsync(): void` | 异步 commit 当前消费位置 |
| `commitBatch(): void` | 整批 commit（`commitAsync` 别名） |
| `close(): void` | 优雅关闭（`kafka:work` 退出前必调） |
| `subscription(): Subscription` | 拿当前订阅描述 |
| `kafka(): \RdKafka\KafkaConsumer` | 拿底层 librdkafka 实例（高级扩展用） |

### 复用

`ConsumerFactory::make($config, $subscription)` 是**单例**（同 config 多次调用返回同一实例）。worker 关闭前调 `$consumer->close()` 释放 fd。

---

## 6. 多 Consumer Group（独立消费同一份消息）

```bash
# 主业务消费 group
php artisan kafka:work --queue=laravel-jobs   # 用 KAFKA_GROUP_ID=laravel-default

# 数据分析消费（独立 group）
KAFKA_GROUP_ID=analytics php artisan kafka:work --queue=laravel-jobs
```

两个 worker 各自独立消费**同一份**消息，互不影响。详细配置：

```dotenv
# .env
KAFKA_GROUP_ID=laravel-default  # 当前 connection 的 group
```

不同 group 用不同 connection：

```php
// config/kafka.php
'connections' => [
    'default' => [
        'group_id' => 'laravel-default',
        // ...
    ],
    'analytics' => [
        'group_id' => 'analytics',
        // 其他可独立
    ],
],
```

```bash
KAFKA_CONNECTION=analytics php artisan kafka:work --queue=laravel-jobs
```

---

## 7. 处理延迟消息（时间轮）

启动 `kafka:delay:work` worker 监听所有 tier topic，到期 requeue 到主 topic：

```bash
# v0.3.1 计划中：当前版本未发布
# php artisan kafka:delay:work --connection=default
```

当前版本（v0.4.1）：`Queue::later()` 写到 tier topic，**需要业务方自己起 worker 监听**。详见 [06-延迟消息](06-延迟消息.md)。

---

## 8. 完整示例：worker 启动脚本

```bash
#!/bin/bash
# /usr/local/bin/laravel-kafka-worker.sh

set -e
cd /var/www/laravel

# 优雅退出处理
trap 'echo "[shutdown] SIGTERM received, waiting worker to drain..."; kill -TERM $WORKER_PID; wait $WORKER_PID' TERM INT

# 启动 worker
php artisan kafka:work \
    --queue=laravel-jobs,order-events,user-events \
    --connection=default \
    --max-time=3600 \
    --max-jobs=100000 \
    --sleep=2 \
    --horizon-metrics \
    &
WORKER_PID=$!

wait $WORKER_PID
```

`supervisor` 配置见 §1。

---

## 下一步

- 失败处理：[05-失败处理](05-失败处理.md)
- 延迟消息：[06-延迟消息](06-延迟消息.md)
- DLQ 运维：[07-DLQ运维](07-DLQ运维.md)
