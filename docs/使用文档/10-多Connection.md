# 10 多 Connection

`Kafka::connection('name')` 多集群管理、`KafkaManager` 缓存与断开。

---

## 1. 概念

`laravel-kafka` 支持多个 Kafka connection（多集群 / 多配置 / 多 group_id），与 Laravel Queue connection 同构。

每个 connection 是**完全独立**的：

- 不同的 `brokers`（不同集群）
- 不同的 `group_id`（独立消费）
- 不同的 `client_id`（broker 端区分）
- 不同的协议（PLAINTEXT vs SASL_SSL）
- 独立的失败处理策略

---

## 2. 配置多连接

```php
// config/kafka.php
'connections' => [
    'default' => [
        'brokers' => 'kafka-main:9092',
        'queue'   => 'laravel-jobs',
        // ... 完整配置
    ],
    'reports' => [
        'brokers'   => 'kafka-reports:9092',
        'client_id' => 'laravel-kafka-reports',
        'queue'     => 'app-reports',
        'consumer'  => [
            'group_id' => 'reports-workers',
            // ... 独立 group
        ],
        // 独立 SSL / SASL（如果有）
        'protocol' => 'SASL_SSL',
        'sasl' => ['mechanism' => 'SCRAM-SHA-512', 'username' => '...', 'password' => '...'],
        'ssl'  => ['ca_location' => '/etc/ssl/ca.pem'],
        // ... 完整配置
    ],
    'audit' => [
        'brokers' => 'kafka-audit:9092',
        'queue'   => 'audit-events',
        'consumer' => ['group_id' => 'audit-archivers'],
        // 单独 retention 长（合规要求）
        'failed' => ['dlq' => ['retention_ms' => 7776000000]],  // 90 天
        // ... 完整配置
    ],
],
```

---

## 3. 使用

### 默认 connection

```php
use LaravelKafka\Facades\Kafka;
use Illuminate\Support\Facades\Queue;

// 通过 Facade 拿
$queue = Kafka::connection();  // = Kafka::connection(null) = 'default'
$config = Kafka::config();

// 通过 Laravel Queue Facade
Queue::push(new MyJob());
```

### 显式指定 connection

```php
// 方式 1：Kafka Facade
Kafka::connection('reports')->push(new ReportJob());

// 方式 2：Laravel Queue Facade（带 connection 参数）
Queue::connection('reports')->push(new ReportJob());

// 方式 3：直接拿 manager
app('kafka.manager')->connection('audit')->push(new AuditEvent($data));
```

### 跨 connection 消费

```bash
# 默认 cluster
php artisan kafka:work --connection=default

# reports cluster（独立 group）
php artisan kafka:work --connection=reports

# audit cluster
php artisan kafka:work --connection=audit
```

或用环境变量：

```bash
# .env
KAFKA_CONNECTION=audit

php artisan kafka:work  # 自动用 audit
```

### 跨 connection 拿 config

```php
$config = Kafka::config('reports');
$config->brokers();         // 'kafka-reports:9092'
$config->consumer()['group_id'];  // 'reports-workers'
```

---

## 4. `KafkaManager` API

```php
$manager = app('kafka.manager');
// 或 $manager = app(\LaravelKafka\Manager\KafkaManager::class);
```

| 方法 | 说明 |
| --- | --- |
| `connection(?string $name = null): Queue` | 拿指定 connection 的 `Queue` 实例（懒加载 + 缓存） |
| `config(?string $name = null): KafkaConfig` | 拿 `KafkaConfig`（不触发连接装配，纯配置层） |
| `disconnect(?string $name = null): void` | 释放 connection（清缓存 + 关 librdkafka fd） |
| `fake(): void` | 切到 fake 模式（不走 Kafka，写 `FakeMessageStorage`） |
| `isFake(): bool` | 查询是否 fake 模式 |
| `registerConnections(array $connections): void` | 业务方**不直接调**（ServiceProvider 启动用） |

---

## 5. 懒加载 + 单例缓存

`KafkaManager` 按 `connection name` 缓存 `Queue` 实例：

```php
// 第一次：触发 KafkaConfig::fromArray() 解析 + ConnectionFactory 装配 producer/consumer/failed
$queue1 = Kafka::connection('reports');

// 第二次：直接返回缓存的 $queue1
$queue2 = Kafka::connection('reports');

// $queue1 === $queue2（同一实例）
```

**性能影响**：
- 第一次调用耗时（装配 librdkafka Conf + 创建 producer/consumer）
- 后续调用 O(1)

业务方一般**不感知**——Laravel Queue 框架内部会缓存。

---

## 6. 释放资源（Octane / 长进程）

`disconnect()` 在以下场景调用：

1. **Laravel Octane** —— 每请求结束释放 fd（避免长进程 fd 泄漏）
2. **测试** —— 同一进程内多次 `Kafka::fake()` 切换（disconnect 重建 manager）
3. **PHP-FPM 短进程** —— **不需要**（进程结束自动释放）

```php
// Octane 场景
public function terminate($request, $response): void
{
    app('kafka.manager')->disconnect('default');
}
```

或单 connection 释放：

```php
app('kafka.manager')->disconnect('reports');
```

---

## 7. 实战示例：电商多集群

业务场景：主业务（订单/支付）+ 数据分析（订单事件镜像）+ 审计（合规归档）。

```php
namespace App\Services;

use LaravelKafka\Facades\Kafka;

class OrderService
{
    public function placeOrder(int $orderId, int $userId): void
    {
        // 1. 主业务：写到 default cluster
        Kafka::connection('default')->push(new ProcessOrder($orderId));

        // 2. 数据分析：镜像订单事件到 reports cluster
        Kafka::connection('reports')->pushRaw(
            json_encode(['event' => 'order.created', 'id' => $orderId]),
            'order-events',
            ['key' => "user-{$userId}"],
        );

        // 3. 审计：合规归档
        Kafka::connection('audit')->push(new AuditOrder($orderId));
    }
}
```

Worker 部署：

```bash
# 3 个独立 worker
php artisan kafka:work --connection=default --queue=laravel-jobs &
php artisan kafka:work --connection=reports --queue=order-events &
php artisan kafka:work --connection=audit --queue=audit-events &

# 3 个独立 group
# default  → KAFKA_GROUP_ID=laravel-default
# reports  → KAFKA_GROUP_ID=reports-workers
# audit    → KAFKA_GROUP_ID=audit-archivers
```

---

## 8. 切换默认 connection

`config('kafka.default')` 控制默认：

```php
// config/kafka.php
'default' => env('KAFKA_CONNECTION', 'default'),
```

```dotenv
# .env
KAFKA_CONNECTION=audit
```

业务方无指定时 `Kafka::connection()` 返回 audit。

---

## 9. 完整示例：业务方动态选 connection

```php
namespace App\Http\Controllers;

use App\Jobs\ProcessOrder;
use App\Jobs\SendReport;
use LaravelKafka\Facades\Kafka;

class TenantController
{
    public function processOrder(int $orderId): void
    {
        $tenant = request()->user()->tenant;

        // 不同租户走不同 connection
        $connection = match ($tenant) {
            'enterprise' => 'reports',
            'audit-only' => 'audit',
            default     => null,  // = default
        };

        Kafka::connection($connection)->push(new ProcessOrder($orderId));
    }
}
```

---

## 下一步

- Serializer：[11-Serializer](11-Serializer.md)
- 事件系统：[12-事件系统](12-事件系统.md)
- KafkaFake 测试：[13-KafkaFake测试](13-KafkaFake测试.md)
