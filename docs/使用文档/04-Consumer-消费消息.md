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

## 1.5 `kafka:work` vs Laravel 8 原生 `queue:work`（重要）

> **TL;DR**：本包**强烈推荐用 `kafka:work`**。Laravel 8 原生 `queue:work` 在 v0.4.6 起**也能消费 Kafka 消息**，但**性能差 5000 倍**（消息延迟 1ms → 5s），仅在**不能换命令名**的极特殊场景下用。

### 行为差异

| 维度 | `kafka:work`（本包命令）| `queue:work`（Laravel 8 原生）|
| --- | --- | --- |
| **实现路径** | 走 `WorkCommand` → `ConsumerFactory::make()` + `Consumer::poll()` 长轮询 | 走 Laravel `Worker` → `KafkaQueue::pop()` 循环 |
| **消息延迟** | **~1 ms**（librdkafka 同步阻塞，broker 主动推 0~1ms 到达）| **~5 秒**（pop 阻塞 1s + Worker sleep 3s + 处理 + 下一轮 1s）|
| **批量消费** | ✅ `--batch-size` / `--batch-timeout` | ❌ 单条循环 |
| **失败处理** | `FailedHandler` 三模式（database / dlq / hybrid）| 同上（Laravel Worker 调 `$job->fail()`）|
| **horizon metrics** | ✅ `--horizon-metrics` 选项 | ❌ |
| **`queue:failed`/`queue:forget` 管理命令** | ✅（v0.4.5 起配 `failed.driver = 'kafka-redis'`）| ✅（同上）|
| **业务方业务方代码兼容性** | ✅ 业务方按 Laravel 标准 Job 写 | ✅ 业务方按 Laravel 标准 Job 写 |

### 为什么 `queue:work` 慢 5000 倍？

```php
// Laravel Worker 循环 (queue:work)
while (true) {
    $job = $connection->pop($queue);     // ← KafkaQueue::pop() 阻塞 1s
    if ($job === null) {
        $this->sleep($options->sleep);    // ← 默认 sleep 3s!
        continue;
    }
    $this->process($connection, $job);    // 处理
}

// kafka:work 循环 (本包)
while (true) {
    $msg = $consumer->consume(1000);     // ← librdkafka 阻塞 1s
    if ($msg) {
        $handler->handle($msg);            // 立即处理
    }
    // 无 sleep: broker 主动推, 消息 0~1ms 到达
}
```

`queue:work` 的 3s sleep 是 Laravel 8 假设 `pop()` 是 Redis/SQL 那种"快查快返"设计的——Kafka 长轮询优势被 sleep 隔断。

### `kafka:work` 调优建议（业务方业务方生产场景）

```bash
# 默认: sleep 1s, 单条处理, 无限运行 — 中等吞吐
php artisan kafka:work

# 高吞吐: 批量消费 + 略长 sleep
php artisan kafka:work --batch-size=50 --batch-timeout=2000 --sleep=2

# 长跑稳定: 1h 自动退出, supervisor 拉新进程
php artisan kafka:work --max-time=3600

# 调试: 跑 1 条退出 (注意: kafka:work 无 --once 选项, 用 --max-jobs=1)
php artisan kafka:work --max-jobs=1
```

### `queue:work` 何时该用？

| 业务方业务方场景 | 推荐 |
| --- | --- |
| 新项目 / 新部署 | ✅ `kafka:work` |
| 业务方业务方业务场景下想"无缝切换 Redis 队列到 Kafka 队列"（不改业务方业务方业务代码）| ✅ `queue:work`（v0.4.6 起 work）|
| 业务方业务方业务场景下需要 Laravel `Horizon` 监控 + 标准 `queue:work` 集成 | ✅ `queue:work` |
| 业务方业务方业务场景下高性能 / 低延迟（秒杀 / 抢购 / 实时数据）| ✅ `kafka:work`（必选）|
| 业务方业务方业务场景下 batch 处理 | ✅ `kafka:work --batch-size=N` |

### 业务方业务方业务场景配置

**`kafka:work`**：在 supervisor / systemd 直接调命令（见上面 §1 典型部署）。

**`queue:work`**：业务方业务方业务场景下需要 `failed.driver = 'kafka-redis'` 让 `queue:failed` / `queue:forget` 命令 work（v0.4.5 已支持）：

```php
// config/queue.php
'failed' => [
    'driver' => 'kafka-redis',
    'connection' => 'default',
    'list_key' => 'kafka:failed_jobs',
    'hash_prefix' => 'kafka:failed_job:',
    'max_items' => 1000,
],
```

**两者用同一个 `KAFKA_GROUP_ID`**：业务方业务方业务场景下两个 worker 一起跑会**抢 partition**——**不要混用**同一 group_id 跑 `kafka:work` + `queue:work`。

### 实测验证

`laravel-test/probe32-queue-work.php` 9/9 全过：
- push 1 条 `StandardOrderJob` → `queue:work --once` 0.18s 真消费，handle log 含 `order_id: 32001`
- `queue:work` 长跑 8s 消费 2 条消息

### 相关变更

- **v0.4.5**：`KafkaQueue::pop()` 永远 null，`queue:work` 完全不能消费
- **v0.4.6**：`pop()` 实现 1000ms 阻塞 poll + 包装 KafkaJob，`queue:work` 能 work（trade-off：5s 延迟）

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

> **v0.5.2 修正**：本包**没有** `kafka.handlers` tag 机制，`HandlerResolver::resolve()` 恒返回
> `NativeHandler`，无 per-topic 数组路由。且 **`HandlerResolver` 是 `final` 类**、
> `WorkCommand::handle()` 构造类型提示具体类——**当前版本自定义 handler 路由暂不支持**。
> per-topic 路由在路线图（v0.6）。裸事件（非 Laravel Job）用 `PayloadReceived` 事件处理
> （[11-Serializer §3](11-Serializer.md#3-发裸事件非-laravel-job消费)），无需自定义 handler。

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

> 上述 handler 类可用，但要路由到它需要 v0.6 的 per-topic 支持。当前所有消息走 `NativeHandler`。

### HandlerResult 三种 action

| action | 触发 | 副作用 |
| --- | --- | --- |
| `ACK` | `HandlerResult::ack()` | commit offset，worker 打印 `<info>ACK</info>` |
| `REQUEUE` | `HandlerResult::requeue()` | **v0.5.2 当前只打印日志**（不重发主 topic），worker 打印 `<comment>REQUEUE</comment>` |
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

`kafka:delay:work` 命令（v0.5.3 已发布）：时间轮延迟消息 worker，监听 tier topic 到期 requeue 回主 topic。启动：`php artisan kafka:delay:work`（独立 group，不抢 kafka:work offset）。详见 [06-延迟消息 §5](06-延迟消息.md)。

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
