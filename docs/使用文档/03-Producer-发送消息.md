# 03 Producer — 发送消息

本章涵盖所有"把消息写到 Kafka"的途径：标准 `Queue::push`、Key 路由保序、自定义 Header、W3C Trace Context、以及直接用低层 `Producer` API。

---

## 1. 标准 `Queue::push`（Laravel 兼容）

业务方用 Laravel 标准方式即可，**无需任何 Kafka 概念**：

```php
use App\Jobs\ProcessOrder;

ProcessOrder::dispatch(['order_id' => 123, 'amount' => 99.5]);
```

等价于：

```php
Queue::push(new ProcessOrder(['order_id' => 123, 'amount' => 99.5]));
```

消息会同步等 broker delivery report（`acks=all`），失败抛 `LaravelKafka\Exceptions\KafkaException`。

---

## 2. `Queue::pushRaw` 跳过 Job 抽象

业务方已有"字符串 payload + 自定义 topic"需求时，跳过 Laravel `Job` 抽象：

```php
$queue = Kafka::connection();  // 或 Queue::connection('kafka')

$queue->pushRaw(
    json_encode(['event' => 'order.created', 'id' => 123]),  // payload 字符串
    'order-events',                                           // 物理 topic
    [
        'key'         => 'user-42',                           // partition 路由
        'traceparent' => '00-aaaa...bbbb-cccc...-01',         // 透传 W3C trace
    ],
);
```

`pushRaw` 的第三个参数 `options`：

| key | 类型 | 说明 |
| --- | --- | --- |
| `topic` | string | 显式覆盖物理 topic（v0.2+） |
| `key` | string | partition 路由键 |
| `traceparent` | string | 透传 W3C trace context |
| `delay_seconds` | int | 延迟秒数（触发时间轮路由） |

---

## 3. Key 路由 — 严格顺序保证

同 key 的消息永远落**同一个** partition，单 consumer 顺序消费。

```php
$queue = Kafka::connection();

// 同一 user 的事件全部按顺序处理
$queue->push($orderJob,    '', 'order-events', "user-{$userId}");
$queue->push($paymentJob,  '', 'order-events', "user-{$userId}");
$queue->push($refundJob,   '', 'order-events', "user-{$userId}");

// 不同 user 走不同 partition 并行处理
$queue->push($jobA, '', 'order-events', 'user-1');
$queue->push($jobB, '', 'order-events', 'user-2');
```

### 工作原理

- `key=null` → librdkafka 默认轮询 partition（**不保证顺序**）
- `key='user-42'` → librdkafka 内部 `consistent_random` 分区器：hash(key) mod num_partitions
- broker 给 partition 分配时这个 hash 一致 → 同 key 永远落同 partition

### 适用场景

- 同一用户的订单 / 支付 / 退款 → 按 `user_id` 路由
- 同一商品的库存变更 → 按 `sku` 路由
- 同一会话的消息 → 按 `session_id` 路由

### 注意事项

- key 基数不能太低（否则 partition 倾斜，单 partition 压力大）
- key 基数也不能太高（百万级 key 会让 partition 路由表过大）
- 推荐 1000 ~ 1000000 之间

---

## 4. 多 Topic 路由

### 方式 A：`config/kafka.php` 长期映射

```php
// config/kafka.php
'connections' => [
    'default' => [
        'queue'   => 'laravel-jobs',
        'topics'  => [
            'emails'   => 'app.emails',
            'reports'  => 'app.reports',
            'audit'    => 'audit.events',
        ],
    ],
],
```

```php
Queue::push($emailJob,  '', 'emails');
Queue::push($reportJob, '', 'reports');
Queue::push($auditJob,  '', 'audit');
```

### 方式 B：业务方临时覆盖

```php
$queue->pushRaw($payload, 'special-topic');              // 一次性指定
$queue->pushRaw($payload, null, ['topic' => 'x.y.z']);   // options 覆盖
```

### 优先级

`options['topic']` > `config.topics[queue]` > `config.queue` 默认

---

## 5. 自定义 Header

业务方有"消息级别元信息"需求时，用 `Message::withHeader()` 链式添加：

```php
use LaravelKafka\Producer\Message;

$msg = new Message(
    payload: json_encode(['order' => 123]),
    headers: ['x-source' => 'web'],
    key: 'user-42',
);

// 链式添加
$msg = $msg
    ->withHeader('x-tenant', 'acme-corp')
    ->withHeader('x-region', 'us-east-1')
    ->withHeaders([
        'x-feature-flag-a' => 'true',
        'x-experiment-id'  => 'exp-2026q3',
    ]);
```

消费端用 `Message::header('x-source')` 读取：

```php
// 在 consumer 端
$source = $message->header('x-source', 'unknown');
```

> 注意：以下 header 由 `KafkaQueue` 自动注入，**不要**覆盖：
> - `x-trace-id` / `traceparent` —— 追踪
> - `x-queue` —— Laravel 逻辑队列
> - `x-connection` —— connection 名
> - `x-enqueued-at` —— 入队时间戳
> - `x-attempt` —— 重试计数
> - `x-serializer` —— 序列化器标识
> - `x-available-at` —— 延迟到期时间

可用常量在 `LaravelKafka\Support\Header`（如 `Header::TRACE_ID`）。

---

## 6. W3C Trace Context 透传

业务方跨服务调用时，把上游的 `traceparent` 透传给 Kafka 消息：

```php
$incomingTraceparent = $request->header('traceparent');  // 从 HTTP 头拿

Queue::pushRaw(
    $payload,
    'order-events',
    [
        'traceparent' => $incomingTraceparent,  // 透传，下游可继续派生子 span
    ],
);
```

消费端读出（自动写入 `traceparent` header）：

```php
$traceparent = $message->header('traceparent');
// '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'
```

> Trace Context 格式说明见 [TraceContext 类源码](../../src/Support/TraceContext.php)（本包自带，
> 不单独建文档章节）。跨服务透传：produce 侧自动注入 `traceparent` header，消费侧
> `NativeHandler` 自动带出，业务方在 handler / 事件监听里读 `$message->header('traceparent')`。

---

## 7. 直接用低层 `Producer` API

业务方已有 librdkafka 经验，想绕开 Laravel Queue 抽象时：

```php
use LaravelKafka\Producer\Producer;
use LaravelKafka\Producer\Message;
use LaravelKafka\Producer\ProducerFactory;

$factory = app(ProducerFactory::class);
$config  = Kafka::config('default');  // 或 Kafka::connection('default')->config()
$producer = $factory->make($config);

// 构造消息
$message = new Message(
    payload: 'hello kafka',
    headers: ['x-source' => 'cli'],
    key: 'demo',
);

// 同步发送（等 delivery report）
try {
    $partition = $producer->send('my-topic', $message);
    echo "Sent to partition $partition\n";
} catch (\LaravelKafka\Exceptions\KafkaException $e) {
    \Log::error('Kafka produce failed', ['error' => $e->getMessage()]);
    throw $e;
}

// 进程退出前必调
$producer->flush(5000);  // 等所有 in-flight 消息投递完成，最多 5s
```

### Producer API 完整列表

| 方法 | 说明 |
| --- | --- |
| `send(string $topic, Message $message): int` | 同步发送，返回 partition 编号 |
| `flush(int $timeoutMs = 10000): void` | 等所有 in-flight 消息投递完成 |
| `kafka(): \RdKafka\Producer` | 拿底层 librdkafka 实例（高级扩展用） |
| `::fromConf(\RdKafka\Conf $conf): self` | 用自定义 Conf 构造（测试用） |
| `initTransactions(int $timeoutMs = 10000): void` | 初始化事务（v0.5.4）|
| `beginTransaction(): void` | 开始事务（v0.5.4）|
| `commitTransaction(int $timeoutMs = 10000): void` | 提交事务（v0.5.4）|
| `abortTransaction(int $timeoutMs = 10000): void` | 回滚事务（v0.5.4）|
| `isTransactional(): bool` | 是否事务模式（v0.5.4）|

### Message 值对象 API

| 方法 | 说明 |
| --- | --- |
| `payload(): string` | 消息体 |
| `headers(): array` | 全部 headers |
| `header(string $name, ?string $default): ?string` | 单个 header |
| `key(): ?string` | partition 路由键 |
| `partition(): ?int` | 显式 partition（**不推荐用**） |
| `timestampMs(): ?int` | 时间戳（null = broker 当前时间） |
| `withHeader(string $name, string $value): self` | 不可变 + 1 个 header |
| `withHeaders(array $headers): self` | 不可变 + 多个 header |
| `withKey(?string $key): self` | 不可变 + 改 key |

### Producer 复用

`ProducerFactory::make($config)` 是**单例**（同 config 多次调用返回同一实例）。业务方不直接 `new Producer`，避免资源泄漏。

---

## 7.5 事务 Producer（v0.5.4）

librdkafka **transactional API**：事务内多条消息原子交付（全成功或全不可见）。

### 配置

```php
// config/kafka.php
'producer' => [
    'enable_idempotence' => true,   // 事务前提（默认已 true）
    'acks'               => 'all',  // 事务前提（默认已 all）
    'transactional_id'   => env('KAFKA_TRANSACTIONAL_ID', ''),
    //  ↑ 唯一事务 id（每 producer 实例一个），空 = 不用事务
],
```

### 用法

```php
use LaravelKafka\Producer\ProducerFactory;

$producer = app(ProducerFactory::class)->make(Kafka::config('default'));

$producer->initTransactions();   // 初始化事务（isTransactional() = true）
$producer->beginTransaction();
try {
    $producer->send('orders', $orderMsg);
    $producer->send('inventory', $inventoryMsg);
    $producer->commitTransaction();   // 原子交付
} catch (\Throwable $e) {
    $producer->abortTransaction();    // 全部回滚
    throw $e;
}
```

### 关键语义

- **事务模式下 `send()` 不等待 delivery report**——消息在 `commitTransaction()` 时才交付，
  同步等 delivery report 会超时（v0.5.4 自动跳过）
- **消费端**必须 `isolation.level = read_committed`（config 默认已配）才能看到**已提交事务**的消息；
  aborted 事务的消息对 read_committed 不可见
- `transactional.id` **必须唯一**（同一 id 多实例会相互踢掉）

### 验证

`laravel-test/probe42-transaction.php`：
- commit 事务 send 2 条 → read_committed consumer 可见
- abort 事务 send 1 条 → read_committed consumer **不可见**（占位 offset 但被跳过）

### 消费端配套

事务 Producer 让生产端"全成功或全不可见"——但消费端要正确处理事务消息（offset 提交时机、失败处理、读 committed 语义）需要专门了解，见 [04 §1.6 事务 Consumer](04-Consumer-消费消息.md#16-事务-consumerv054-配套)。

---

## 8. 同步 vs 异步

| 模式 | API | 适用 |
| --- | --- | --- |
| **同步**（默认） | `Producer::send()` 循环 `poll(50)` 等 delivery report | 业务方希望"成功即落 broker，失败立刻抛" |
| **异步** | `Producer::kafka()->producev()` 自己 flush | 业务方已有异步架构，要批量攒消息 |

本包强制 `send()` 同步等待。**异步用法**用低层 librdkafka API 自己管。

---

## 9. 完整示例：电商订单事件流

```php
namespace App\Services;

use LaravelKafka\Facades\Kafka;
use LaravelKafka\Producer\Message;

class OrderEventPublisher
{
    public function publishCreated(int $orderId, int $userId, float $amount): void
    {
        $payload = json_encode([
            'event'      => 'order.created',
            'order_id'   => $orderId,
            'user_id'    => $userId,
            'amount'     => $amount,
            'created_at' => now()->toIso8601String(),
        ], JSON_THROW_ON_ERROR);

        // 同 user 顺序 + 业务自定义 header
        Kafka::connection()->pushRaw(
            $payload,
            'order-events',
            [
                'key'   => "user-{$userId}",
                'traceparent' => request()->header('traceparent', ''),
            ],
        );
    }
}
```

使用：

```php
$publisher = app(OrderEventPublisher::class);
$publisher->publishCreated(orderId: 12345, userId: 42, amount: 99.5);
```

---

## 下一步

- 消费侧：[04-Consumer-消费消息](04-Consumer-消费消息.md)
- 失败处理：[05-失败处理](05-失败处理.md)
- 延迟消息：[06-延迟消息](06-延迟消息.md)
