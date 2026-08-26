# 13 KafkaFake 单元测试

`Kafka::fake()` + 4 种断言 API，让单元测试不依赖真实 Kafka broker。

---

## 1. 概念

业务方在测试场景下不想起 broker，但仍要验证：

- "下单后应该发 `orders.created` 事件"
- "发送的消息 payload 包含 `amount=100`"
- "没有消息被发"（条件分支）

`Kafka::fake()` 切换到 fake 模式，`Queue::push()` 不会真发 Kafka，只把消息记录到内存 `FakeMessageStorage`。测试用例通过 `KafkaFake` 实例做断言。

---

## 2. 基本用法

```php
namespace Tests\Feature;

use Tests\TestCase;
use App\Jobs\SendOrderEmail;
use LaravelKafka\Facades\Kafka;

class OrderTest extends TestCase
{
    public function test_order_creation_publishes_event(): void
    {
        Kafka::fake();

        // 业务代码（通过 Laravel Queue facade 推消息）
        $this->postJson('/api/orders', [
            'user_id' => 42,
            'amount'  => 100,
        ]);

        // 断言 1：orders.created topic 至少 1 条
        Kafka::assertPushedOn('orders.created');

        // 断言 2：payload 包含 amount=100
        Kafka::assertPushedOn('orders.created', function (string $topic, \LaravelKafka\Producer\Message $msg): bool {
            return str_contains($msg->payload(), 'amount=100');
        });
    }
}
```

---

## 3. 4 种断言 API

### 3.1 `assertPushed(?string $topic = null, ?callable $callback = null): void`

**断言**至少 1 条 push 匹配。

| 参数 | 说明 |
| --- | --- |
| `$topic` | 期望的物理 topic，`null` = 任意 |
| `$callback` | 真值测试，签名 `function (string $topic, Message $message): bool` |

```php
// 任意 topic 至少 1 条
Kafka::assertPushed();

// 指定 topic 至少 1 条
Kafka::assertPushed('orders.created');

// 指定 topic + payload 校验
Kafka::assertPushed('orders.created', function ($topic, $msg) {
    return $msg->key() === 'user-42'
        && str_contains($msg->payload(), 'amount=100');
});
```

### 3.2 `assertPushedTimes(int $times, ?string $topic = null, ?callable $callback = null): void`

**断言**恰好 N 条 push 匹配。

```php
// orders.created 恰好 1 条
Kafka::assertPushedTimes(1, 'orders.created');

// orders.created 恰好 3 条 + 都是 user-42
Kafka::assertPushedTimes(3, 'orders.created', function ($topic, $msg) {
    return $msg->key() === 'user-42';
});
```

### 3.3 `assertPushedOn(string $topic, ?callable $callback = null): void`

`assertPushed($topic, $callback)` 的语义糖：明确写"在指定 topic"。

```php
// orders.created 至少 1 条
Kafka::assertPushedOn('orders.created');
```

### 3.4 `assertPushedOnTimes(string $topic, int $times, ?callable $callback = null): void`

`assertPushedTimes($times, $topic, $callback)` 的语义糖。

```php
// orders.created 恰好 2 条
Kafka::assertPushedOnTimes('orders.created', 2);
```

### 3.5 `assertNothingPushed(): void`

**断言**没有任何 push 调用（最严格）。

```php
// 条件分支：用户是禁用账户时不应该发邮件
public function test_disabled_user_no_email(): void
{
    Kafka::fake();

    $disabledUser = \App\Models\User::factory()->create(['disabled' => true]);
    $this->actingAs($disabledUser)->postJson('/api/orders', [...]);

    Kafka::assertNothingPushed();
}
```

---

## 4. 完整断言

```php
Kafka::fake();

// 触发业务代码
$controller->placeOrder($orderId);

Kafka::assertPushedOn('orders.created');                    // ✓ 至少 1 条
Kafka::assertPushedOnTimes('orders.created', 1);           // ✓ 恰好 1 条
Kafka::assertPushedOn('orders.created', function ($t, $m) { // ✓ payload 校验
    return $m->key() === "user-{$order->user_id}"
        && str_contains($m->payload(), '"order_id":' . $order->id);
});
```

---

## 5. callback 中能访问的 `Message` 属性

```php
$callback = function (string $topic, \LaravelKafka\Producer\Message $msg): bool {
    return
        // topic
        $topic === 'orders.created'

        // payload
        && str_contains($msg->payload(), 'order_id=123')

        // key
        && $msg->key() === 'user-42'

        // headers（5+ 业务方 + Kafka 注入）
        && $msg->header('x-queue') === 'orders'
        && $msg->header('x-attempt') === '0'
        && $msg->header('x-trace-id') !== null
        && $msg->header('traceparent') !== null

        // timestamp
        && $msg->timestampMs() > 0

        // partition
        && $msg->partition() === null;  // null = UDA
};
```

---

## 6. 拿到 `KafkaFake` 实例

业务方想遍历所有消息做更复杂断言：

```php
public function test_complex_order_event(): void
{
    $fake = Kafka::fake();

    $this->postJson('/api/orders', ['user_id' => 42, 'amount' => 100]);

    // 拿到 storage（$fake->storage() v0.5 才有，v0.4.1 用 reflection）
    // 或者：用 assertPushed + callback 完成
    Kafka::assertPushedOn('orders.created', function ($t, $m) {
        $data = json_decode($m->payload(), true);
        return $data['event'] === 'order.created'
            && $data['user_id'] === 42
            && $data['amount'] === 100;
    });
}
```

> v0.4.1 当前：`Kafka::fake()` 返回 `KafkaFake` 实例，但 `storage` 是 `private`。
> v0.5 公开 `storage()` getter 让业务方能直接遍历。

---

## 7. fake 模式的边界

### 哪些会被 fake 拦截

✅ `Queue::push($job)` → `KafkaQueue::pushRaw` → fake 模式分支 → 记录到 `FakeMessageStorage`
✅ `Queue::pushRaw($payload, $topic, $options)` → 同上
✅ `Queue::later($seconds, $job)` → fake 模式**不**走 `DelayRouter`，直接 pushRaw 到原 topic（延迟特性在 fake 下不模拟）

### 哪些**不**被 fake 拦截

❌ 直接用 `Producer::send()`（绕过 `KafkaQueue`）—— 走真发路径
❌ Kafka 事件 dispatch（`MessagePublishing` 等）—— fake 模式下仍 dispatch（业务方 listener 自己处理）
❌ Consumer 端 / `kafka:work` —— 单元测试不跑 worker

### 强制走 fake

业务方有"直接用 Producer 而非 Queue"的需求时，**不**会被 fake 拦截。要拦截必须先 `Kafka::fake()`，然后：

```php
public function test_uses_low_level_producer(): void
{
    Kafka::fake();

    // 直接用 Producer —— **不会**被 fake 拦截
    $producer = app(\LaravelKafka\Producer\ProducerFactory::class)
        ->make(Kafka::config('default'));
    $producer->send('orders.created', new \LaravelKafka\Producer\Message('payload'));

    Kafka::assertNothingPushed();  // 失败：实际 producer 绕过 fake 真发了（或抛 Connection refused）
}
```

> 业务方测试时**统一用 Queue facade**，不要直接用 Producer。

---

## 8. fake 模式 + 多 connection

```php
public function test_multi_connection(): void
{
    Kafka::fake();

    Queue::push(new \App\Jobs\ProcessOrder($id));                    // 'default' connection
    Queue::connection('reports')->push(new \App\Jobs\SendReport($id));  // 'reports' connection

    // 跨 connection 断言
    Kafka::assertPushedOnTimes('orders.created', 1);  // 'default' 的 topic
    Kafka::assertPushedOnTimes('app-reports', 1);     // 'reports' 的 topic
}
```

> fake 模式下所有 connection 共享一个 `FakeMessageStorage`（不分 connection 隔离）。

---

## 9. fake 模式 + Laravel Queue::fake() 协同

业务方有 `Queue::fake()`（Laravel 内置）和 `Kafka::fake()`（本包）：

```php
public function test_order_dispatch(): void
{
    Queue::fake();  // Laravel 拦截 Job dispatch
    Kafka::fake();  // 本包拦截 Kafka push

    // 业务代码
    $controller->placeOrder($id);

    // Laravel Queue::fake 断言
    Queue::assertPushed(ProcessOrder::class);

    // 本包 Kafka::fake 断言（独立于 Laravel Queue::fake）
    Kafka::assertPushedOn('orders.created');
}
```

两个 fake **独立**：

- `Queue::fake()` 让 `Bus::dispatch` 不真跑 `handle()`，但 `KafkaQueue::pushRaw` 仍真发 Kafka
- `Kafka::fake()` 让 `KafkaQueue::pushRaw` 不真发，但 `Bus::dispatch` 仍真跑 `handle()`

业务方按需选一个或两个都用。

---

## 10. 业务方在 PestPHP 中的写法

```php
use LaravelKafka\Facades\Kafka;

it('publishes order created event', function () {
    Kafka::fake();

    $this->postJson('/api/orders', ['user_id' => 42, 'amount' => 100]);

    Kafka::assertPushedOn('orders.created');
});
```

---

## 11. 完整示例：电商订单测试

```php
namespace Tests\Feature;

use Tests\TestCase;
use App\Jobs\ProcessOrder;
use App\Jobs\SendOrderEmail;
use LaravelKafka\Facades\Kafka;

class OrderControllerTest extends TestCase
{
    public function test_successful_order_publishes_event_and_dispatches_jobs(): void
    {
        Kafka::fake();

        $response = $this->postJson('/api/orders', [
            'user_id' => 42,
            'items'   => [['sku' => 'ABC', 'qty' => 1]],
            'amount'  => 99.5,
        ]);

        $response->assertOk();

        // Kafka 事件
        Kafka::assertPushedOnTimes('orders.created', 1);
        Kafka::assertPushedOn('orders.created', function ($t, $m) {
            $data = json_decode($m->payload(), true);
            return $m->key() === 'user-42'
                && $data['amount'] === 99.5;
        });

        // 同一订单的 created 事件只发一次
        Kafka::assertPushedTimes(1, 'orders.created');
    }

    public function test_disabled_user_no_event(): void
    {
        Kafka::fake();

        $user = \App\Models\User::factory()->create(['disabled' => true]);
        $this->actingAs($user)->postJson('/api/orders', [...]);

        Kafka::assertNothingPushed();
    }

    public function test_high_value_order_does_not_publish_event(): void
    {
        Kafka::fake();

        $this->postJson('/api/orders', [
            'user_id' => 42,
            'amount'  => 1.0,  // 小额订单不发外部事件
        ]);

        Kafka::assertNothingPushed();
    }
}
```

---

## 下一步

- 安全连接：[14-安全连接](14-安全连接.md)
- CLI 命令清单：[15-CLI命令清单](15-CLI命令清单.md)
