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

## 1.6 事务 Consumer（v0.5.4 配套）

> **配套阅读**：事务 Producer 见 [03 §7.5](03-Producer-发送消息.md#75-事务-producerv054)。

事务 Producer 让生产端"全成功或全不可见"——但消费端要正确处理事务消息，需要了解 4 件事。

### 1. 核心配置：`isolation.level=read_committed`

```php
// config/kafka.php
'consumer' => [
    'isolation_level' => env('KAFKA_ISOLATION_LEVEL', 'read_committed'),
    //                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    // 关键: 不看到 aborted 事务消息, 只看到已 commit 的事务消息
],
```

- **`read_committed`**（生产必选）：aborted 事务消息**不可见**；commit 消息可见
- **`read_uncommitted`**（仅调试）：能看 aborted 占位；破坏业务原子语义，**生产禁用**

> ⚠️ 本包 config 默认就是 `read_committed`，业务方**不要**改成 `read_uncommitted`，否则事务语义失效。

### 2. 核心语义：consumer 看到了什么

```
Broker 事务流                                       read_committed consumer 看到
─────────────────────────────────────────          ──────────────────────────────
TXN-1: send 2 条 → begin                          (空)
TXN-1: commit                                     2 条消息按 send 顺序
TXN-2: send 1 条 → begin                          (空)
TXN-2: abort                                      (空, 占位 offset 跳过)
TXN-3: send 3 条 → begin                          (空)
TXN-3: commit                                     3 条消息按 send 顺序
```

**关键事实**：

1. **同一事务的消息按 send 顺序到达 consumer**（broker 端 partition 内有序）
2. **跨事务的消息严格按 commit 顺序**（TXN-1 commit 早于 TXN-3 → consumer 看到 TXN-1 早于 TXN-3）
3. **aborted 消息占 offset 但不返回**（consumer poll 时被 librdkafka 内部过滤）
4. **consumer 端没有"事务"概念**——看到的还是单条消息，只是中间穿插了"事务边界"
   - librdkafka 内部维护 `LSO`（Last Stable Offset），read_committed consumer poll 时跳过 LSO 之前的未 commit 消息
   - 业务方在 Handler 里**完全不需要**关心事务边界，按正常单条消息处理即可

### 3. offset 提交时机（最重要的实践）

```
                  业务方 handler 处理
                  ↓
  poll 消息 ────→ 写 DB / 调 RPC / 通知 ────→ handler 返回 true
                                                 ↓
                                          KafkaJob::delete()
                                          (commit offset 到 broker)
```

**原则**：offset 提交**必须**在 handler 成功处理完**之后**。

- ✅ handler 处理成功 → `KafkaJob::delete()` → 提交 offset → 业务才算完成
- ❌ poll 到消息就 commit offset → 处理失败时 offset 已提交 → **消息丢失**

`kafka:work` 已经在 `KafkaJob::delete()` 里手动 commit offset（不依赖 librdkafka 自动 commit），业务方只需要：

- handler 正常 return → 框架自动 commit offset
- handler 抛异常 → 框架走重试 / DLQ（v0.1 failed 三模式），**不会** commit offset
- handler 调 `$job->fail($e)` → 写 failed_jobs，commit offset，**不**再重试

> 不要在自己的 handler 里手动 commit offset。`KafkaJob::delete()` 是唯一入口。

### 4. 失败处理：consumer 端没有 abort，只能 DLQ

| 场景 | Producer 端 | Consumer 端 |
|---|---|---|
| 业务正常 | `commitTransaction()` | handler return → offset commit |
| 业务异常 | `abortTransaction()` | handler throw → 重试 / DLQ |

**Consumer 端没有"事务回滚"概念**——因为消费端没有"已经 commit 的消息可以撤回"这种语义。能做的只有：

```php
// NativeHandler 写法
$handler = new NativeHandler(function ($message) {
    $payload = json_decode($message->payload(), true);

    // 业务校验失败 → 抛异常 → 框架走 DLQ
    if (empty($payload['user_id'])) {
        throw new \InvalidArgumentException('user_id 缺失');
    }

    // 调外部 RPC 失败 → 抛异常 → 框架走重试 (max_attempts 内)
    $this->httpClient->post('https://inventory.example.com/decrement', $payload);
});
```

- **可重试异常**（网络抖动、临时 5xx）→ Kafka 失败处理按 `max_attempts` 重试
- **不可重试异常**（参数缺失、数据格式错）→ 在 `fatal_exceptions` 列表里 → 直接 DLQ 不再重试
- **业务侧需要"精确一次"消费** → 用 `x-idempotency-key` header 业务层去重（v0.6 backlog）

### 5. 完整示例：消费事务消息

#### 方式 A：Laravel Job（推荐）

`app/Jobs/ProcessOrderCreated.php`：

```php
<?php

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

class ProcessOrderCreated implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public function __construct(public array $orderData) {}

    public function handle(): void
    {
        // 业务处理: 发邮件 / 更新统计 / 通知仓库
        \Log::info('Order created', $this->orderData);
    }

    // 可选: 失败时的钩子
    public function failed(\Throwable $e): void
    {
        // 业务方通知: Slack / 钉钉 / PagerDuty
        \Log::error('Order processing failed', [
            'order_id' => $this->orderData['order_id'] ?? null,
            'error'    => $e->getMessage(),
        ]);
    }
}
```

生产端发事件：

```php
// app/Http/Controllers/OrderController.php
use App\Jobs\ProcessOrderCreated;
use App\Events\InventoryDecremented;

$producer->beginTransaction();
try {
    // 写 orders 事件 (Laravel Job 格式)
    Queue::pushOn('orders-events', new ProcessOrderCreated($orderData));

    // 写 inventory 事件 (裸 JSON 事件)
    $producer->send('inventory-events', new Message(
        json_encode(['event' => 'inventory.decremented', 'sku' => $sku, 'delta' => -1]),
        ['x-serializer' => 'json'],
        $sku,
    ));

    $producer->commitTransaction(15000);
} catch (\Throwable $e) {
    $producer->abortTransaction(15000);
    throw $e;
}
```

消费端 worker：

```bash
# 订阅 orders-events (Laravel Job 格式), 走 failed handler
php artisan kafka:work --queue=orders-events

# 订阅 inventory-events (裸 JSON 格式), 用 NativeHandler
php artisan kafka:work --queue=inventory-events
```

#### 方式 B：NativeHandler（裸事件，不走 Laravel Job）

`app/Kafka/Handlers/InventoryDecrementedHandler.php`：

```php
<?php

namespace App\Kafka\Handlers;

use LaravelKafka\Consumers\NativeHandler;

class InventoryDecrementedHandler extends NativeHandler
{
    public function handle(\LaravelKafka\Producer\Message $message): void
    {
        $payload = json_decode($message->payload(), true);

        // 业务处理: 同步更新本地缓存 / 触发下游
        Cache::decrement("inventory:{$payload['sku']}", abs($payload['delta']));
    }
}
```

注册到 kafka:work（v0.6 backlog 里有 `kafka.handlers` 路由数组，目前用 `--handler` 选项单 handler）：

```bash
php artisan kafka:work \
    --queue=inventory-events \
    --handler="App\Kafka\Handlers\InventoryDecrementedHandler"
```

### 6. 验证：怎么确认 read_committed 工作正常

`laravel-test/probe42-transaction.php` 验证：

```
✅ commit 事务 send 2 条 → read_committed consumer 看到 2 条
✅ abort  事务 send 1 条 → read_committed consumer 看到 0 条
```

业务方本地跑：

```bash
cd laravel-test
php probe42-transaction.php
# 看到 7 OK, 0 FAIL 即可
```

### 7. 常见误区（必看）

| 误区 | 实际 |
|---|---|
| "Consumer 端需要调 commitTransaction 提交" | ❌ Consumer 端没有事务 API。事务只在 Producer 端 |
| "abort 消息会进 DLQ" | ❌ aborted 消息在 broker 端被跳过，read_committed consumer 根本看不到 |
| "可以在 handler 里手动 commit offset 提前释放" | ❌ 会丢消息。handler 成功 return → 框架自动 commit；失败 throw → 框架走重试 / DLQ |
| "我可以用 read_uncommitted 看 aborted 消息做调试" | ⚠️ 仅本地调试。生产用 → 破坏业务原子语义 |
| "事务消息会乱序到达 consumer" | ❌ 同一 partition 内严格按 send 顺序；跨事务按 commit 顺序 |

### 8. 与 Producer 事务的对照

| 维度 | Producer 端 | Consumer 端 |
|---|---|---|
| 事务 API | `beginTransaction` / `commitTransaction` / `abortTransaction` | 无（只能 commit offset / DLQ） |
| 可见性控制 | `transactional_id`（必填） | `isolation_level`（默认 read_committed） |
| 失败回滚 | `abortTransaction()`（消息全不可见）| 抛异常 → 重试 / DLQ（消息已 commit 不可撤回）|
| 消息保证 | 原子（要么全 commit 要么全 abort） | 至少一次（重试） + 业务去重（幂等）= 精确一次 |
| 关键配置 | `producer.transactional_id` | `consumer.isolation_level` |

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
