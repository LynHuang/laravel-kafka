# laravel-kafka

[![PHP](https://img.shields.io/badge/PHP-7.4%2B-777bb4)](https://www.php.net)
[![Laravel](https://img.shields.io/badge/Laravel-8%20%7C%209%20%7C%2010%20%7C%2011-ff2d20)](https://laravel.com)
[![ext-rdkafka](https://img.shields.io/badge/ext--rdkafka-required-orange)](https://github.com/arnaud-lb/php-rdkafka)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![CI](https://img.shields.io/badge/CI-7.4%20%7C%208.1%20%7C%208.3%20%7C%20Laravel%208--11-blue)](.github/workflows/tests.yml)
[![Packagist](https://img.shields.io/badge/packagist-lyn--huang%2Flaravel--kafka-blue)](https://packagist.org/packages/lyn-huang/laravel-kafka)

> Apache Kafka Queue driver & event bus for Laravel.
> **PHP 7.4+** · 强依赖 `ext-rdkafka` · 支持 Laravel 8 / 9 / 10 / 11。

---

## 这是什么

`laravel-kafka` 是基于 [librdkafka](https://github.com/confluentinc/librdkafka) 的 Laravel 队列驱动 + 事件总线：

- **完全兼容 Laravel Queue 契约** —— `Queue::push / later / pop`，Laravel 业务代码**零改动**即可切换
- **三种失败处理模式** —— `database` (Laravel `failed_jobs` 表) / `dlq` (独立 topic) / `hybrid` (重试入库 + 超限 DLQ)
- **Key 路由保序** —— 同 key 落同分区，分区消费严格顺序
- **时间轮分层延迟消息** —— 不用 broker 端定时器，topic 分层 + `kafka:delay:work` worker
- **DLQ 高级特性** —— 异常类路由 (`ExceptionClassRouter`) + 滑动窗口限速 (`DlqRateLimiter`)
- **批量消费** —— `pollBatch` + `commitBatch` 整批原子语义
- **W3C Trace Context** —— 完整 `traceparent` 头，跨服务透传
- **回溯 Replay** —— 按时间窗口重放 topic
- **Horizon 5.x 兼容** —— 复用 Horizon Lua 脚本，metrics 写到 `horizon:` 前缀
- **KafkaFake 测试** —— 不用起 broker 也能断言 push 调用

📖 **详细功能与示例**：[`docs/使用文档/README.md`](docs/使用文档/README.md)（按功能逐块展开，含可运行代码，共 16 篇）

---

## 目录

- [快速上手](#快速上手)
- [系统要求](#系统要求)
- [版本演进](#版本演进)
- [文档索引](#文档索引)
- [License](#license)

---

## 快速上手

### 1. 安装 `ext-rdkafka`

```bash
# macOS
brew install librdkafka && pecl install rdkafka

# Ubuntu / Debian
apt-get install librdkafka-dev && pecl install rdkafka

# 验证
php -m | grep rdkafka   # 应输出 rdkafka
```

### 2. Composer 安装

```bash
composer require lyn-huang/laravel-kafka
```

包安装后会自动注册 `LaravelKafkaServiceProvider` 和 `Kafka` Facade（通过 `composer.json` 的 `extra.laravel` 段）。

### 3. 发布配置文件

```bash
php artisan vendor:publish --tag=kafka-config
```

这会复制 `config/kafka.php` 到项目根 `config/` 目录。

### 4. 配置 `.env`

```dotenv
QUEUE_CONNECTION=kafka

KAFKA_BROKERS=localhost:9092
KAFKA_DEFAULT_TOPIC=laravel-jobs
KAFKA_GROUP_ID=laravel-default
KAFKA_FAILED_DRIVER=hybrid
```

### 5. 在 `config/queue.php` 增加 kafka 驱动

默认安装后 `kafka` 驱动已被 `Queue::extend('kafka', ...)` 注册，无需手动改 `queue.php`。如需指定默认 connection：

```php
// config/queue.php
'default' => env('QUEUE_CONNECTION', 'kafka'),
```

### 6. 启动 worker

```bash
php artisan kafka:work --queue=laravel-jobs
```

业务方 `Queue::push(new MyJob())` 即可写入 Kafka，`kafka:work` 长驻消费。

> 完整配置项、Worker 命令选项、失败处理、延迟、Replay、Horizon 适配、单元测试 Fake 等：见 [📖 docs/使用文档/README.md](docs/使用文档/README.md)

---

## 系统要求

| 组件 | 最低版本 | 推荐 |
| --- | --- | --- |
| PHP | 7.4 | 8.1+ |
| ext-rdkafka | 任意 | ≥ 5.0 + librdkafka ≥ 1.5 |
| Laravel | 8.x | 11.x |
| Kafka broker | 0.11+ | 2.5+（KRaft 单节点 OK） |

**Composer require**：

```json
"require": {
    "php": ">=7.4",
    "ext-rdkafka": "*",
    "illuminate/queue": "^8.0 || ^9.0 || ^10.0 || ^11.0",
    "illuminate/support": "^8.0 || ^9.0 || ^10.0 || ^11.0",
    "illuminate/console": "^8.0 || ^9.0 || ^10.0 || ^11.0",
    "illuminate/contracts": "^8.0 || ^9.0 || ^10.0 || ^11.0",
    "nesbot/carbon": "^2.0 || ^3.0",
    "ramsey/uuid": "^4.0"
}
```

**PHP 7.4 限制**（强制遵守）：

- ❌ 不支持 `enum` / `match` / `readonly` / 属性提升 / 命名参数 / nullsafe 操作符 / `#[Attribute]`
- ❌ 不支持 `trailing_comma` 在**函数调用**和**方法定义**参数列表末尾（PHP 8.0+ 才有）
- ❌ 不支持 `Readonly` 属性 / `mixed` 类型的复杂用法

---

## 版本演进

完整变更日志见 [`docs/CHANGELOG.md`](docs/CHANGELOG.md)。下面是按能力维度的小结。

### v0.1 — Laravel Queue 驱动基石

| 能力 | 说明 |
| --- | --- |
| ✅ Laravel Queue 完整契约 | `Queue::push / later / pop`，Laravel 业务代码零改动 |
| ✅ 三种失败处理 | `database` / `dlq` / `hybrid` 可配 |
| ✅ Consumer Group 水平扩展 | 多个 worker 进程消费同一 group，自动 partition 互斥 |
| ✅ 协议支持 | PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL |
| ✅ 强类型配置 | `KafkaConfig` 不可变值对象，IDE 友好 |
| ✅ `Message` 值对象 | payload + headers + key + partition + timestamp |
| ✅ 同步 produce | `Producer::send` 同步等 delivery report，失败抛 `KafkaException` |
| ✅ 优雅退出 | `kafka:work` 监听 SIGTERM / SIGINT，处理完当前消息再退出 |
| ✅ 单元测试基类 | `LaravelKafka\Tests\TestCase` 继承 Orchestra Testbench |

### v0.2 — 高级消息能力

| 能力 | 说明 |
| --- | --- |
| ✅ 多 Topic 路由 | `config('kafka.connections.*.topics')` 队列名 → topic 映射 |
| ✅ Key Routing 顺序保证 | `Queue::push($job, '', 'queue', 'user-42')` 同 key 落同分区 |
| ✅ 独立 DLQ topic | `<topic>.dlq` 默认后缀，可自定义 |
| ✅ 延迟消息（v0.2 同步阻塞版，v0.3 升级为时间轮） | `Queue::later(60, $job)` |
| ✅ Header Trace / 透传 | `x-trace-id` 16hex + 透传支持 |
| ✅ KafkaFake 测试 | `Kafka::fake() + assertPushedOn` 系列断言 |
| ✅ Kafka Facade | `Kafka::connection() / config() / fake() / disconnect()` |
| ✅ Laravel 事件 | `MessagePublishing` / `MessagePublished` |
| ✅ 多个 connection | `Kafka::connection('reports')` 多集群 |

### v0.3 — 生产级增强

| 能力 | 说明 |
| --- | --- |
| ✅ 时间轮分层延迟 | `tiers=[5,30,60,300,1800,3600,86400]` 秒，broker 持久化 |
| ✅ 批量消费 | `kafka:work --batch-size=50` + `pollBatch` + `commitBatch` |
| ✅ DLQ 异常路由 | `ExceptionClassRouter` 按异常类分发到不同 DLQ topic |
| ✅ DLQ 限速 | `DlqRateLimiter` 滑动窗口每分钟 N 条 |
| ✅ DLQ tail | `kafka:dlq:tail <topic>` 不 commit 实时打印 |
| ✅ Replay CLI | `kafka:replay --topic=x --from=-1h --to=now` 时间窗口重放 |
| ✅ 时间窗口解析 | `TimeWindowParser` 支持 `now` / `-1h` / `1700000000` / `2026-08-25` 格式 |

### v0.4 — Horizon 兼容

| 能力 | 说明 |
| --- | --- |
| ✅ Horizon metrics | 复用 Horizon 5.x Lua 脚本，metrics 写到 `horizon:` 前缀 Redis key |
| ✅ `kafka:work --horizon-metrics` | 启动选项，启用后 `NativeHandler` 自动写 throughput + runtime |
| ✅ `kafka:horizon:snapshot` | 模板化命令，业务方通常直接用 Horizon 自带 `horizon:snapshot` |

### v0.5（路线图）

候选方向（[CHANGELOG](docs/CHANGELOG.md) `[Unreleased]` 段）：

- `kafka:delay:work` worker + 业务代码接入
- 事务 Producer（librdkafka transactional API）
- 幂等性（`enable.idempotence=true` + 应用层 idempotency key）
- 多 Consumer Group Fan-out 完善
- Schema Registry / Avro 集成
- OpenTelemetry SDK 替换手写 traceparent
- Octane 适配

---

## 文档索引

| 文档 | 用途 |
| --- | --- |
| 📖 [**docs/使用文档/README.md**](docs/使用文档/README.md) | **详细功能 + 示例**（16 篇，按主题分块，推荐阅读入口） |
| 📋 [docs/CHANGELOG.md](docs/CHANGELOG.md) | 完整版本变更日志 |

---

## 常见问题（FAQ）

**Q1：业务方已有 Laravel 项目，迁移到 Kafka queue 麻烦吗？**
零代码改动。`composer require` + 改 `.env` 的 `QUEUE_CONNECTION=kafka` + 启动 `kafka:work` worker 即可。Laravel 自己的 `Job` 类、`ShouldQueue` 接口、`Bus::dispatch` 全部不变。

**Q2：怎么保证消息按顺序处理？**
用 Key Routing：`Queue::push($job, '', 'queue-name', 'user-42')`，同 key 永远落同 partition，单 consumer 顺序消费。同 partition 内 librdkafka 严格有序。

**Q3：失败任务怎么排查？**
取决于 `KAFKA_FAILED_DRIVER` 配置：
- `database` / `hybrid`：`php artisan queue:failed` + `queue:retry <uuid>`
- `dlq` / `hybrid`：`php artisan kafka:dlq:tail laravel-jobs.dlq` 实时打印

**Q4：能从历史时间点重放消息吗？**
v0.3 起：`php artisan kafka:replay --topic=orders.events --from=-1h --to=now --target-topic=orders.events.replay`（v0.3 MVP 只做窗口校验，实际 reproduce 留 v0.5）。

**Q5：能跨语言消费吗？**
能。Kafka 协议天然多语言。本包 header 透传 W3C Trace Context + Laravel 标准 payload（PHP serialize），其他语言用 Kafka client 拉消息后自行反序列化（如果需要跨语言 payload 互通，可用 `JsonSerializer`）。

**Q6：跟 mateusjunges/laravel-kafka 有什么区别？**

| 维度 | mateusjunges | 本包 |
| --- | --- | --- |
| 命名空间 | `Junges\Kafka` | `LaravelKafka\` |
| 最低 PHP | 8.0 | **7.4** |
| 失败处理 | 仅 DLQ | database / dlq / **hybrid**（重试 + DLQ） |
| 延迟消息 | 内存定时器（重启丢） | **时间轮分层 topic**（broker 持久化） |
| 批量消费 | 需手动管理 | `pollBatch` + `commitBatch` 整批原子 |
| 异常路由 | 不支持 | `ExceptionClassRouter` 异常类 → DLQ topic 路由 |
| DLQ 限速 | 不支持 | `DlqRateLimiter` 滑动窗口 |
| Trace | 无 | **W3C Trace Context** 完整透传 |
| 配置文件 | 散落 `Manager` | `KafkaConfig` 不可变值对象，IDE 补全友好 |
| Horizon 兼容 | 需自己写 | **复用 Horizon 5.x Lua 脚本** |

---

## License

[MIT](LICENSE) © 2026 Lyn-Huang
