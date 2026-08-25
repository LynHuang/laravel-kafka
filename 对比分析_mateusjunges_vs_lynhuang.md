# `mateusjunges/laravel-kafka` vs `lyn-huang/laravel-kafka` 对比分析

> **分析对象**：
> - **A 项目**：[`mateusjunges/laravel-kafka`](https://github.com/mateusjunges/laravel-kafka) — Laravel 社区最知名的 Kafka 扩展（v3.x，2024-2026 主流）
> - **B 项目**：本工作区 `lyn-huang/laravel-kafka` — 我们 v0.1 脚手架（2026-08-20）
>
> **数据源**：所有结论基于**真实 fetch 的 GitHub 源码**（commit `a128a2c`），不是二手博客印象。
>
> **核心结论**：两个项目**解决不同问题**——
> - A 是"**工业级 Kafka 客户端**"，与 Laravel Queue 并存
> - B 是"**Laravel Queue 驱动替代品**"，沿用 Queue 抽象
>
> 但可以互相借鉴。**§6 是给 v0.2 提的借鉴清单**。

---

## 0. 一句话差异

| 项目 | 一句话定位 |
| --- | --- |
| **mateusjunges/laravel-kafka** | "Laravel 项目里**额外**加一个 Kafka 客户端，原生 Kafka API + Builder 流式 API" |
| **lyn-huang/laravel-kafka** | "**替换** Laravel 的 Redis/database Queue 驱动，让业务零改动用上 Kafka" |

---

## 1. 18 个维度对比

### 1.1 基础元信息

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **Composer 名** | `mateusjunges/laravel-kafka` | `lyn-huang/laravel-kafka` |
| **命名空间** | `Junges\Kafka\` | `LaravelKafka\` |
| **PHP 版本** | `^8.2 \|\| ^8.3 \|\| ^8.4 \|\| ^8.5` | `>=7.4` |
| **Laravel 版本** | `^12.0 \|\| ^13.0` | `^8.0 \|\| ^9.0 \|\| ^10.0 \|\| ^11.0` |
| **ext-rdkafka** | `^6.0`（锁版本） | `*`（不锁） |
| **License** | MIT | MIT |
| **GitHub 可见性** | 公开（社区主流） | 私有（v0.1 阶段） |
| **依赖 illuminate/queue** | ❌（不要） | ✅（必须） |
| **额外依赖** | monolog, 自家 avro-serde-php | ramsey/uuid |
| **ServiceProvider 路径** | `src/Providers/LaravelKafkaServiceProvider.php` | `src/LaravelKafkaServiceProvider.php` |
| **package.json 风格** | `minimum-stability: dev`（激进） | `minimum-stability: stable`（保守） |
| **代码风格工具** | `laravel/pint` + `rector/rector` | `friendsofphp/php-cs-fixer` + `phpstan/phpstan` |

### 1.2 核心架构

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **设计模式** | Builder 流式 API | Queue 驱动 + Manager/Factory |
| **核心入口** | `Kafka::publish()->onTopic()->...->send()` | `Queue::push(new MyJob())` |
| **消费者入口** | `Kafka::consumer([topics])->withHandler()->build()->consume()` | `kafka:work` Artisan 命令 |
| **消息模型** | 可变（`Message` 类 + Arrayable） | 不可变（`Producer\Message`） |
| **配置对象** | `Config` 类（值对象，由 Builder 构造） | `KafkaConfig` 值对象 + `config('kafka.*')` 数组 |
| **Manager** | `Factory` 实现 `Manager` 契约（带 `shouldFake` 状态） | `KafkaManager` + `ConnectionFactory` 拆分 |
| **依赖注入** | 大量用 `app()` / `App::make()` 静态调用 | 显式构造函数注入为主 |

### 1.3 API 风格对比

**mateusjunges 流式 Builder API**（`src/Producers/Builder.php`）：

```php
use Junges\Kafka\Facades\Kafka;

Kafka::publish('localhost:9092')
    ->onTopic('orders.created')
    ->withKafkaKey((string) $order->id)
    ->withBodyKey('order_id', $order->id)
    ->withBodyKey('amount', $order->amount)
    ->withHeaders(['tenant' => 'acme', 'trace_id' => $traceId])
    ->withConfigOption('compression.codec', 'snappy')
    ->withSasl('user', 'pass', 'PLAIN', 'SASL_SSL')
    ->withTransactionalId('orders-svc-1')
    ->send();
```

**lyn-huang Laravel Queue 标准 API**（`src/Queue/KafkaQueue.php`）：

```php
use Illuminate\Support\Facades\Queue;

Queue::push(new ProcessOrder($orderId, $amount));  // 业务代码 0 改动
```

`ProcessOrder` 是 Laravel 标准 `Illuminate\Contracts\Queue\ShouldQueue` Job 类。

### 1.4 失败处理

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **失败处理定位** | 库内**内建** DLQ 通道 | 三模式可配（database / dlq / hybrid） |
| **触发时机** | Consumer 内部 `handleException()` 自动调 | `KafkaJob::fail()` 主动调 handler |
| **DLQ topic 命名** | 默认 `<topic>-dlq`（自动拼接） | 默认 `laravel-jobs.dlq`（用户可配） |
| **DLQ header** | 3 个（message / code / class） | 9 个（topic / partition / offset / headers / failed_at / exception / message / trace / attempts） |
| **写 failed_jobs 表** | ❌（不集成 Laravel failed_jobs） | ✅（hybrid 模式） |
| **与 `queue:failed` 集成** | ❌ | ✅（`syncFailedTableConfig` 桥接） |
| **fatal exception 概念** | ❌ | ✅（hybrid 模式可配） |
| **DLQ 后是否 commit** | ✅（`$this->committer->commitDlq($message)`） | ✅（`$this->consumer->ack($this->rdMessage)`） |

### 1.5 事件系统

mateusjunges 有 **6 个事件**，lyn-huang v0.1 只有 **1 个占位**：

| 事件 | mateusjunges | lyn-huang |
| --- | --- | --- |
| `PublishingMessage` | ✅（produce 前） | ❌ |
| `MessagePublished` | ✅（produce 后） | ❌ |
| `StartedConsumingMessage` | ✅（consume 前） | ❌ |
| `MessageConsumed` | ✅（consume 后） | ❌ |
| `MessageSentToDLQ` | ✅（DLQ 写入时） | ❌ |
| `CouldNotPublishMessage` | ✅（发送失败） | ❌ |
| `JobFailed` 监听器 | ❌ | ✅（占位） |

**业务影响**：

mateusjunges 业务方能这样用：
```php
Event::listen(MessagePublished::class, function ($event) {
    Metrics::counter('kafka_publish_total')->tag('topic', $event->message->getTopicName())->inc();
});
```

lyn-huang v0.1 没这个能力——业务方只能 `error_log` 或读 `failed_jobs` 表。

### 1.6 测试支持

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **Fake 模式** | ✅ `Kafka::fake()` 内建 | ❌ |
| **断言 API** | `assertPublished / assertPublishedOn / assertPublishedTimes / assertNothingPublished` | ❌（只有 Testbench 单元测试） |
| **集成测试** | Testbench + 真实 Kafka | Testbench + Testcontainers CI 占位 |

mateusjunges 的 `KafkaFake`（`src/Support/Testing/Fakes/KafkaFake.php`）核心：

```php
class KafkaFake
{
    use ForwardsCalls;
    private array $publishedMessages = [];

    public function assertPublished(?ProducerMessage $expectedMessage = null, ?callable $callback = null): void
    {
        PHPUnit::assertTrue(
            condition: $this->published($callback, $expectedMessage)->count() > 0,
            message: 'The expected message was not published.'
        );
    }

    public function assertPublishedTimes(int $times = 1, ...): void
    public function assertPublishedOn(string $topic, ...): void
    public function assertNothingPublished(): void
}
```

**对比业务影响**：

mateusjunges 业务方：
```php
public function test_order_creation_publishes_event(): void
{
    Kafka::fake();
    
    $this->post('/orders', [...]);
    
    Kafka::assertPublishedOn('orders.created', function ($message) {
        return $message->getBodyKey('order_id') === 123;
    });
}
```

lyn-huang v0.1 业务方：只能 `Bus::fake()` + 自己 mock Producer + 验证 KafkaJob 被 dispatch，**不能**验证 Kafka payload。

### 1.7 Consumer 抽象

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **Handler 接口** | `Handler` 契约 + `CallableConsumer` 包装类 | `HandlerInterface` + `NativeHandler`（Laravel Worker 桥） |
| **中间件** | ✅ `withMiddleware()` 链 | ❌ |
| **before / after 回调** | ✅ `beforeConsuming()` / `afterConsuming()` | ❌ |
| **手动 commit 模式** | ✅ `withManualCommit()` | ❌（v0.1 强制 `auto_commit=false`） |
| **错误码分类** | 4 类常量（IGNORABLE_CONSUMER_ERRORS / CONSUME_STOP_EOF_ERRORS / TIMEOUT_ERRORS / IGNORABLE_COMMIT_ERRORS） | 仅 2 类（PARTITION_EOF / TIMED_OUT 当 null） |
| **Max Messages 限制** | ✅（带 `MessageCounter`） | ❌（v0.1 不支持） |
| **stopAfterLastMessage** | ✅（自动停） | ❌ |
| **Consumer 重启** | ✅ `RestartConsumersCommand` + `Cache` 跨进程协调 | ❌ |

### 1.8 Commit 策略

**mateusjunges 有完整的 `Committer` 抽象**（`src/Commit/` 目录）：

| Committer | 用途 |
| --- | --- |
| `VoidCommitter` | 不 commit（测试 / 调试） |
| `DefaultCommitterFactory` | 默认 |
| `BatchCommitter` | 批量 commit |
| `RetryableCommitter` | commit 失败重试 |
| `SeekToCurrentErrorCommitter` | 错误时 seek 回当前 offset |

lyn-huang v0.1：**没有** Committer 抽象，`KafkaJob::delete()` 直接调 `$consumer->ack($rdMessage)`，策略硬编码。

### 1.9 Schema Registry / Avro

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **Avro Serializer/Deserializer** | ✅ v3 已有 | ❌ v0.4 占位 |
| **Schema Registry 客户端** | ✅ 5 个文件完整实现 | ❌ |
| **额外依赖** | `mateusjunges/avro-serde-php ^3.0` | 无 |

### 1.10 事务

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **事务 Producer** | ✅ `withTransactionalId()` + `transactional()` | ❌ v0.3 占位 |
| **Concerns\ManagesTransactions** | ✅ 完整实现 | ❌ |
| **事务重试** | `withTransactionalId` + `maxTransactionRetryAttempts` | ❌ |

### 1.11 序列化

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **默认 Serializer** | `JsonSerializer` | `PhpSerializer`（与 Laravel 兼容） |
| **可自定义** | ✅ `usingSerializer()` | ✅ v0.1 已留接口 |
| **Avro** | ✅ | ❌ |
| **消息 ID 自动生成** | ✅ `kafka.message_id_key` header | ✅ `x-trace-id` header |

### 1.12 启动模式

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **CLI 入口** | `kafka:consume --consumer=...` + `kafka:restart-consumers` | `kafka:work` |
| **手动管理** | 用户写 PHP 代码 `Kafka::consume()->build()->consume()` 在自己的命令里 | 我们直接 `php artisan kafka:work` |
| **多 Consumer 调度** | `RestartConsumersCommand` 协调多个 consumer 进程 | 单 consumer 进程 |

### 1.13 Rebalance 策略

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **可配置 rebalance strategy** | ✅ `RebalanceStrategy` enum（range / roundrobin / sticky / cooperative-sticky） | ❌（v0.1 硬编码 librdkafka 默认） |
| **自定义 partition 分配** | ✅ `assignPartitions()` / `assignPartitionsWithOffsets()` | ❌ |

### 1.14 错误处理

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **统一异常类** | `LaravelKafkaException` + 5 个子类（ConsumerException / CouldNotPublishMessage / MessageIdNotSet / SchemaRegistryException / 2 个 Transaction 异常） | `KafkaException` + `SerializationException` + `DlqException` |
| **flush 重试** | ✅ `retry()` 辅助函数 + `flush_retries` + `flush_retry_sleep_in_ms` | ❌（v0.1 5s 硬同步等） |
| **Retryable** | ✅ `Retryable` 类 + `NativeSleeper` | ❌ |

### 1.15 中间件

mateusjunges 的中间件系统（`src/Concerns/PrepareMiddlewares.php`）：

```php
Kafka::consumer(['orders'])
    ->withMiddleware(MyMetricsMiddleware::class)
    ->withMiddleware(function ($message, $consumer) {
        Log::info('processing message', ['offset' => $consumer->getLastOffset()]);
    })
    ->withHandler(function (ConsumedMessage $message) {
        // 业务处理
    })
    ->build()
    ->consume();
```

**中间件可以做**：metrics、tracing、auth、rate limiting、消息校验……

lyn-huang v0.1 没有中间件概念——业务方要这些只能 `around($message, function () { ... })` 写在 Handler 里。

### 1.16 异步 Producer

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **async 模式** | ✅ `Kafka::asyncPublish()` | ❌ |
| **destructor 自动 flush** | ✅ `__destruct` 在 `async` 时 flush | ❌ |
| **flush callback** | ✅ `withFlushCallback()` | ❌ |

### 1.17 文档

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **用户文档** | 外链 `laravelkafka.com`（独立站） | 内联 README |
| **设计文档** | 无（演进靠代码） | ✅ `开发文档_v0.1.md`（15 章） |
| **实施日志** | 无 | ✅ `docs/开发日志_v0.1.md`（14 步） |
| **源码精读教程** | 无 | ✅ `LaravelKafka扩展开发教程.md`（14 步） |
| **概念入门** | docs/ 子目录（advanced-usage / producing / consuming / testing） | ✅ `Kafka入门教程.md` |
| **CHANGELOG** | 36 KB 自动生成（git-cliff） | ✅ `docs/CHANGELOG.md` 4 KB 手写 |
| **RFC** | 无 | ✅ `RFC/0001-initial.md` / `0002-meta.md` |

### 1.18 公开度

| 维度 | mateusjunges | lyn-huang |
| --- | --- | --- |
| **GitHub 仓库** | 公开 | 私有 |
| **Packagist** | 公开，可 `composer require mateusjunges/laravel-kafka` | 需 vcs 源 |
| **贡献者** | 社区开放 | 仅 owner |
| **CI 公开 badge** | ✅ | ❌（私有仓库无 badge） |

---

## 2. mateusjunges 5 个我们 v0.1 缺的关键特性

按 **业务价值 / 实现成本** 排序：

### 2.1 KafkaFake（极高价值 / 中等成本）

**业务价值**：
- 单元测试能验证 Kafka payload 完整内容
- 不需要真 Kafka 集群跑测试
- 测试速度：本地 ms 级 vs Testcontainers 30s 级

**mateusjunges 实现**（精简）：
```php
class KafkaFake
{
    use ForwardsCalls;
    private array $publishedMessages = [];
    
    public function __call(string $method, array $parameters)
    {
        $this->kafkaManager->shouldReceiveMessages($this->messagesToConsume);
        return $this->forwardCallTo($this->kafkaManager, $method, $parameters);
    }
    
    public function assertPublished(...): void { PHPUnit::assertTrue(...); }
}
```

**借鉴实现成本**：中。需要：
- `Factory` 加 `shouldFake` 状态
- `ProducerBuilderFake` / `ConsumerBuilderFake` 包装类
- Fake 注入到容器（`Container::extend`）
- 7-8 个 `assertXxx` 断言方法

**v0.2 建议**：✅ 列入 v0.2 必做

### 2.2 6 个事件系统（高价值 / 中等成本）

**业务价值**：
- metrics / tracing / audit 接入零侵入
- 与 Laravel 生态的 Event / Listener 体系对接
- 解耦业务代码

**mateusjunges 实现**（精简）：
```php
class MessagePublished
{
    public function __construct(public ProducerMessage $message) {}
}

// Producer:
$this->dispatcher->dispatch(new PublishingMessage($message));
$this->topic->producev(...);
$this->dispatcher->dispatch(new MessagePublished($message));
```

**借鉴实现成本**：中。每个事件是一个简单值对象 + Producer/Consumer 各加 1-2 行 dispatch。

**v0.2 建议**：✅ 列入 v0.2 必做

### 2.3 Committer 抽象（中价值 / 高成本）

**业务价值**：
- 批量 commit 提升吞吐
- 失败重试 commit
- 测试用 VoidCommitter

**mateusjunges 实现**：5 个 Committer + 1 个 Factory

**借鉴实现成本**：高。需要重构 `KafkaJob::delete()`，把 ack 调用从硬编码改为接口委托。

**v0.2 建议**：⚠️ 列入 v0.2 评估，v0.3 落地。v0.1 量级没必要。

### 2.4 中间件链（中价值 / 中等成本）

**业务价值**：
- 横切关注点（metrics / auth / 限流）解耦
- 测试时可以插入 mock middleware

**借鉴实现成本**：中。需要新增 `Middleware` 接口 + `MiddlewareStack` 调度 + `withMiddleware()` Builder 方法。

**v0.2 建议**：⚠️ 列入 v0.2 评估，等 v0.3 事件系统稳定后做。

### 2.5 RestartConsumersCommand + Cache 协调（低价值 / 高成本）

**业务价值**：
- supervisor 重启时所有 consumer 自动重读 config
- 不用手动 kill -HUP

**借鉴实现成本**：高。需要：
- `kafka:restart-consumers` 命令
- 启动时 `Cache::get('laravel-kafka:consumer:restart')` 读版本号
- 心跳机制：每 N 秒 `Cache::increment()`

**v0.2 建议**：❌ v0.2 不做。属于运维增强，不是核心能力。

---

## 3. 我们 5 个 mateusjunges 不做的差异化能力

| 能力 | 说明 | mateusjunges 状态 |
| --- | --- | --- |
| **作为 Laravel Queue 驱动** | `Queue::push(new MyJob())` 业务零改动 | ❌ 必须用 `Kafka::publish()->...->send()` |
| **复用 `failed_jobs` 表** | hybrid 模式 + `syncFailedTableConfig` 桥接 `queue:failed` 命令 | ❌ DLQ 是唯一渠道 |
| **PHP 7.4+ 兼容** | 大量老企业项目 | ❌ 要 8.2+ |
| **Laravel 8/9/10 兼容** | 老版本 LTS | ❌ 要 12/13 |
| **3 模式失败处理可配** | database / dlq / hybrid 切换 | ❌ 只有 DLQ 一种 |
| **9 个 DLQ 头部** | 系统化的失败上下文 | ❌ 只有 3 个 throwable header |
| **ext-rdkafka 不锁版本** | 适配系统 librdkafka | ❌ 锁 `^6.0` |
| **内联中文文档三件套** | 设计 + 实施 + 教程 | ❌ 只有外链英文文档 |

**最大差异化**：我们能**让业务方零改动**把现有 Laravel Queue 切到 Kafka——这是 mateusjunges 做不到的。

---

## 4. 共同点

5 个**两边都认可的最佳实践**：

1. **强依赖 ext-rdkafka**——避开 pure-PHP 实现的坑
2. **不可变/可序列化 Message 值对象**——避免副作用
3. **手动 commit 默认**——`auto.commit=false` + 业务成功后 commit
4. **SASL / SSL 通过 protocol 自动注入字段**——避免 librdkafka 报"未知配置"
5. **Testbench 做单元测试基线**——不依赖真 Kafka 跑通大多数测试

---

## 5. 给 v0.2 借鉴清单（按 ROI 排序）

| ROI | 特性 | 来源 | 建议版本 |
| --- | --- | --- | --- |
| ⭐⭐⭐⭐⭐ | `KafkaFake` + 7 个断言 | §2.1 | **v0.2 必做** |
| ⭐⭐⭐⭐⭐ | 6 个事件 | §2.2 | **v0.2 必做** |
| ⭐⭐⭐⭐ | 9 个 DLQ header 保留（**我们已有**） | – | 已是 v0.1 |
| ⭐⭐⭐⭐ | 3 模式失败处理（**我们已有**） | – | 已是 v0.1 |
| ⭐⭐⭐⭐ | Middleware 链 | §2.4 | v0.2 评估 |
| ⭐⭐⭐ | Avro + Schema Registry | §1.9 | v0.3 评估 |
| ⭐⭐⭐ | Transaction Producer | §1.10 | v0.3 评估 |
| ⭐⭐⭐ | 异步 Producer | §1.16 | v0.3 评估 |
| ⭐⭐ | Committer 抽象 | §2.3 | v0.3 评估 |
| ⭐⭐ | MaxMessages 限制 | §1.7 | v0.2 评估 |
| ⭐⭐ | stopAfterLastMessage | §1.7 | v0.2 评估 |
| ⭐ | RebalanceStrategy 枚举 | §1.13 | v0.4 评估 |
| ⭐ | RestartConsumersCommand | §2.5 | v0.4 评估 |

---

## 6. 关键源码引用

为了不空口无凭，每个核心结论都引真实文件。已 fetch 的临时文件在工作区根 `mj*` 前缀。

| 结论 | 引用的 mateusjunges 源文件 | 行号 |
| --- | --- | --- |
| Builder 流式 API | `mjsrc_Producers_Builder.php` | L72-204 |
| Producer + Dispatcher 集成 | `mjsrc_Producers_Producer.php` | L33-77 |
| 6 个事件 dispatch | `mjsrc_Producers_Producer.php` | L56, L146 |
| Consumer 内部自动 DLQ | `mjsrc_Consumers_Consumer.php` | L348-356 |
| DLQ header 3 个 | `mjsrc_Consumers_Consumer.php` | L389-404 |
| `Kafka::fake()` API | `mjsrc_Support_Testing_Fakes_KafkaFake.php` | L79-117 |
| 4 类错误码分类 | `mjsrc_Consumers_Consumer.php` | L38-57 |
| Async publish + destructor | `mjsrc_Factory.php` | L47-63 |
| ServiceProvider 极简 | `mjsrc_Providers_LaravelKafkaServiceProvider.php` | L23-48 |
| 不依赖 illuminate/queue | `mjcomposer.json` | L5-12 |
| `noz_allowed_queues: false`（设计哲学） | `mjconfig_kafka.php` | 全文扁平 |

| 结论 | 引用的 lyn-huang 源文件 | 行号 |
| --- | --- | --- |
| Queue 驱动继承 | `src/Queue/KafkaQueue.php` | 全文 |
| 9 个 DLQ header | `src/Queue/Failed/DlqFailedJobHandler.php` | L31-51 |
| 三模式失败处理 | `src/Queue/Failed/HybridFailedJobHandler.php` | 全文 |
| Laravel failed_jobs 桥接 | `src/LaravelKafkaServiceProvider.php` | `syncFailedTableConfig()` |
| PHP 7.4 兼容 | `composer.json` | L9 `php: ">=7.4"` |
| KafkaJob::delete 提交 offset | `src/Queue/KafkaJob.php` | L78-82 |

---

## 7. 实施建议（v0.2 启动前）

### 7.1 短期（v0.2 必做）

1. **从 mateusjunges 抄 `KafkaFake`**——但适配我们的 Queue 驱动语义
   - 我们没有 `Kafka::publish()`，但可以在 `KafkaJob` 上挂 fake
   - 业务方 `$this->app['kafka.manager']->fake()` 后，`Queue::push()` 不会真发 Kafka
   - 业务方 `Kafka::assertPushed(ProcessOrderJob::class, function ($job) { ... })` 验证

2. **从 mateusjunges 抄 6 个事件**——但我们的 4 个事件更对：
   - `MessagePublishing`（替代 `PublishingMessage`，前缀更通用）
   - `MessagePublished`
   - `MessageConsuming`
   - `MessageConsumed`
   - `MessageFailed`
   - `MessageSentToDLQ`

3. **保留我们的差异化**：
   - 9 个 DLQ header（他们 3 个）
   - 三模式失败处理（他们 1 种）
   - Queue 驱动入口（他们 Builder）

### 7.2 中期（v0.3 评估）

- Middleware 链
- Async Producer
- Avro + Schema Registry
- Transaction Producer

### 7.3 长期（v0.4 评估）

- Committer 抽象重构
- RebalanceStrategy 枚举
- RestartConsumersCommand

### 7.4 不要做的

- ❌ 跟 mateusjunges 走一样的 Builder API
  - 我们是 Queue 驱动，强行做 Builder 是"为不同而不同"
- ❌ 删 `failed_jobs` 表集成
  - 我们的差异化就是与 Laravel failed_jobs 兼容

---

## 8. 结论

**两个项目不冲突**：
- mateusjunges/laravel-kafka = 工业级 Kafka 客户端（**与 Laravel Queue 并存**）
- lyn-huang/laravel-kafka = Laravel Queue 驱动的 Kafka 替代品（**替换** Redis/database queue）

**借鉴的 5 个特性**（v0.2-v0.4 逐步落地）：
1. KafkaFake 测试体系
2. 完整事件系统
3. Middleware 链
4. Committer 抽象
5. 多错误码分类

**坚持的差异化**：
1. Queue 驱动入口
2. 三模式失败处理
3. 9 个 DLQ header
4. PHP 7.4+ 兼容
5. 内联中文文档三件套

**总目标**：做**Laravel 生态最易上手的 Kafka 队列驱动**，同时不丢 Kafka 特色能力。
