# 11 Serializer

`PhpSerializer` / `JsonSerializer` 选择 + 自定义序列化器。

---

## 1. 概念

`Serializer` 接口定义 produce 端 encode / consume 端 decode 的统一契约。消费端根据消息 header `x-serializer` 自动选对应反序列化器。

### 为什么需要这个

- 默认 Laravel `Job::createPayload()` 序列化成 PHP serialize 字符串
- 但跨语言消费（Go / Python / Java）需要 JSON / Avro
- 不同 topic 用不同 serializer 互不干扰

---

## 2. 内置两种实现

### `PhpSerializer`（默认）

```php
use LaravelKafka\Producer\Serializer\PhpSerializer;

$serializer = new PhpSerializer();

$payload = $serializer->encode(['order_id' => 123, 'amount' => 99.5]);
// 'a:2:{s:7:"order_id";i:123;s:6:"amount";d:99.5;}'

$value = $serializer->decode($payload);
// ['order_id' => 123, 'amount' => 99.5]

$serializer->name();  // 'php'
```

**适用**：

- 纯 PHP 生态（Laravel 单体应用）
- 消息内容是 Job 抽象（默认 Laravel Queue payload 格式）
- 兼容性最广（Laravel 内置 Job 都用 PHP serialize）

### `JsonSerializer`

```php
use LaravelKafka\Producer\Serializer\JsonSerializer;

$serializer = new JsonSerializer();

$payload = $serializer->encode(['order_id' => 123, 'amount' => 99.5]);
// '{"order_id":123,"amount":99.5}'

$value = $serializer->decode($payload);
// ['order_id' => 123, 'amount' => 99.5]

$serializer->name();  // 'json'
```

**适用**：

- 跨语言消费（其他语言用 stdlib JSON parser）
- 直接发业务事件（不是 Laravel Job）

---

## 3. 发裸事件（非 Laravel Job）+ 消费

> **v0.5.0 接入**：`NativeHandler` 真正支持按 `x-serializer` header 选反序列化器。
> 裸事件（非 Laravel Job payload）→ `JsonSerializer::decode` → dispatch `PayloadReceived` 事件。
> **v0.5.1 配置化**：默认 Serializer 通过 `config/kafka.php` 配置。

### 3.0 配置默认 Serializer（v0.5.1）

`config/kafka.php`：

```php
'connections' => [
    'default' => [
        // ... 其他配置
        'serializer' => env('KAFKA_SERIALIZER', 'php'),  // php | json
    ],
],
```

或 `.env`：

```dotenv
KAFKA_SERIALIZER=json
```

**作用**：
- **push 侧**：`KafkaQueue::buildMessage` 的 `x-serializer` header 用配置值（替代 v0.4.1 硬编码 `'php'`）
- **consume 侧**：`NativeHandler` 裸事件无 `x-serializer` header 时用配置默认解码

**优先级**（consume 侧）：消息 `x-serializer` header（显式）> 配置默认 > PhpSerializer fallback。



业务方用 `JsonSerializer` 发业务事件（跨语言消费场景），绕过 `Queue::push`，直接用低层 `Producer` API：

```php
use LaravelKafka\Producer\Producer;
use LaravelKafka\Producer\Message;
use LaravelKafka\Producer\Serializer\JsonSerializer;

$serializer = new JsonSerializer();

$producer = app(\LaravelKafka\Producer\ProducerFactory::class)
    ->make(Kafka::config('default'));

$producer->send('order-events', new Message(
    payload: $serializer->encode(['event' => 'order.created', 'id' => 123]),
    headers: ['x-serializer' => $serializer->name()],  // 消费端按此选反序列化器
    key: 'user-42',
));
```

消费端 `NativeHandler` 检测到消息**不是** Laravel Job payload（无 `data.command`）时，
按 `x-serializer` header 选反序列化器解码，然后 dispatch `PayloadReceived` 事件：

```php
// app/Providers/EventServiceProvider.php
use LaravelKafka\Events\PayloadReceived;

Event::listen(PayloadReceived::class, function (PayloadReceived $event) {
    // $event->payload() = ['event' => 'order.created', 'id' => 123]
    // $event->topic()   = 'order-events'
    Log::info('收到业务事件', $event->payload());
});
```

**重要**：`Queue::push` / `dispatch` 发的 Laravel Job 走 `Worker::process`，**不**触发
`PayloadReceived`。裸事件和 Laravel Job 可以在同一 `kafka:work` worker 里混合消费。

### Laravel Job 的序列化限制

`Queue::createPayload` 输出 JSON（外层 `{"uuid", "job", "data.command"}`），其中
`data.command` 是 **PHP serialize 字符串**——这是 Laravel 框架内部格式，跨语言消费者
读不懂 `data.command`。**想让 Node/Go/Python 消费，用裸事件（本节）而非 Laravel Job**。

---

## 4. 自定义 Serializer

### Step 1：实现 `Serializer` 接口

```php
namespace App\Kafka\Serializers;

use LaravelKafka\Exceptions\SerializationException;
use LaravelKafka\Producer\Serializer\Serializer;

class AvroSerializer implements Serializer
{
    /**
     * @var \App\Kafka\Avro\SchemaRegistry
     */
    private $registry;

    public function __construct(\App\Kafka\Avro\SchemaRegistry $registry)
    {
        $this->registry = $registry;
    }

    public function encode($value): string
    {
        try {
            $schema = $this->registry->getSchemaFor($value);
            $writer = new \Avro\IO\BinaryEncoder();
            $datumWriter = new \Avro\IODatumWriter($schema);
            $datumWriter->write($value, $writer);
            return $writer->toString();
        } catch (\Throwable $e) {
            throw new SerializationException(
                sprintf('Avro encode failed: %s', $e->getMessage()),
                0,
                $e,
            );
        }
    }

    public function decode(string $raw)
    {
        try {
            $schema = $this->registry->getLatestSchema();
            $reader = new \Avro\IO\BinaryDecoder($raw);
            $datumReader = new \Avro\IODatumReader($schema);
            return $datumReader->read($reader);
        } catch (\Throwable $e) {
            throw new SerializationException(
                sprintf('Avro decode failed: %s', $e->getMessage()),
                0,
                $e,
            );
        }
    }

    public function name(): string
    {
        return 'avro';
    }
}
```

### Step 2：绑定到容器

```php
// app/Providers/AppServiceProvider.php
public function register(): void
{
    $this->app->singleton(AvroSerializer::class, function ($app) {
        return new AvroSerializer($app->make(\App\Kafka\Avro\SchemaRegistry::class));
    });
}
```

### Step 3：消费端按 `x-serializer` 选反序列化器

`NativeHandler` 默认 registry 含 `php`（PhpSerializer）+ `json`（JsonSerializer）。
业务方有自定义时用 `registerSerializer()` 注册（v0.5.0 提供）：

```php
// app/Providers/AppServiceProvider.php
use LaravelKafka\Consumer\Handler\NativeHandler;

public function boot(): void
{
    app()->resolving(NativeHandler::class, function (NativeHandler $handler) {
        $handler->registerSerializer('avro', app(AvroSerializer::class));
    });
}
```

裸事件消费时按 `x-serializer` header 选对应序列化器：`avro` → AvroSerializer，
`json` → JsonSerializer，未匹配 → fallback 到默认 PhpSerializer。

---

## 5. 异常处理

`encode()` / `decode()` 失败必须抛 `LaravelKafka\Exceptions\SerializationException`（**不**抛 `RuntimeException` / `Exception`），这样 `HybridFailedJobHandler` 的 `fatal_exceptions` 配置才能精确识别。

```php
public function encode($value): string
{
    try {
        return $this->doEncode($value);
    } catch (\Throwable $e) {
        throw new SerializationException(
            sprintf('Custom encode failed: %s', $e->getMessage()),
            0,        // code
            $e,       // previous
        );
    }
}
```

业务方在 `fatal_exceptions` 配 `SerializationException::class`，序列化错误**直接**进 DLQ（不重试）：

```php
'failed' => [
    'driver' => 'hybrid',
    'hybrid' => [
        'fatal_exceptions' => [
            \LaravelKafka\Exceptions\SerializationException::class,
        ],
        'max_attempts' => 3,
    ],
],
```

---

## 6. 完整示例：业务方多 Serializer 混用

业务方有 3 类消息：

- `laravel-jobs` —— Laravel Job（PHP serialize）
- `order-events` —— 跨语言事件（JSON）
- `audit-events` —— Avro 强 schema 治理

```php
class OrderPublisher
{
    public function __construct(
        private ProducerFactory $factory,
        private JsonSerializer $json,
        private AvroSerializer $avro,
    ) {}

    public function publishOrderCreated(int $orderId, int $userId): void
    {
        $producer = $this->factory->make(Kafka::config('default'));

        $producer->send('order-events', new Message(
            payload: $this->json->encode(['event' => 'order.created', 'id' => $orderId]),
            headers: ['x-serializer' => $this->json->name()],
            key: "user-{$userId}",
        ));

        $producer->send('audit-events', new Message(
            payload: $this->avro->encode(['order_id' => $orderId, 'amount' => 99.5]),
            headers: ['x-serializer' => $this->avro->name()],
        ));
    }
}
```

消费端 `CustomNativeHandler` 按 `x-serializer` 选 `php` / `json` / `avro` 反序列化器。

---

## 7. 跨语言示例

### Python 消费 JSON

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'order-events',
    bootstrap_servers='localhost:9092',
    group_id='python-consumer',
    auto_offset_reset='earliest',
)

for msg in consumer:
    # 消息是 JSON 字符串
    data = json.loads(msg.value)
    print(f"Order {data['id']} created")
```

### Python 消费 Avro

```python
from kafka import KafkaConsumer
import io
import fastavro

# 读 Avro schema
with open('order.avsc') as f:
    schema = fastavro.parse_schema(json.load(f))

consumer = KafkaConsumer(
    'audit-events',
    bootstrap_servers='localhost:9092',
    group_id='python-audit',
    auto_offset_reset='earliest',
)

for msg in consumer:
    bytes_io = io.BytesIO(msg.value)
    record = fastavro.schemaless_reader(bytes_io, schema)
    print(f"Audit: {record}")
```

### Go 消费 JSON

```go
package main

import (
    "encoding/json"
    "github.com/segmentio/kafka-go"
)

type OrderEvent struct {
    Event   string `json:"event"`
    OrderID int    `json:"id"`
}

func main() {
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "order-events",
        GroupID: "go-consumer",
    })

    for {
        m, err := reader.ReadMessage(context.Background())
        if err != nil {
            break
        }
        var event OrderEvent
        json.Unmarshal(m.Value, &event)
        fmt.Printf("Order %d %s\n", event.OrderID, event.Event)
    }
}
```

---

## 下一步

- 事件系统：[12-事件系统](12-事件系统.md)
- KafkaFake 测试：[13-KafkaFake测试](13-KafkaFake测试.md)
- 安全连接：[14-安全连接](14-安全连接.md)
