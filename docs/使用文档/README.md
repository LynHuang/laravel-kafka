# laravel-kafka 使用文档

> 详细功能讲解 + 可运行示例。按主题分块，建议按顺序阅读或按需查阅。

---

## 目录

### 入门

| # | 文档 | 内容 |
| --- | --- | --- |
| 01 | [快速上手](01-快速上手.md) | 5 分钟跑通：安装 ext-rdkafka → composer require → 第一个 push → 第一个 worker |
| 02 | [配置详解](02-配置详解.md) | `config/kafka.php` 全部字段 + `.env` 环境变量 + `config/queue.php` 关联 |

### 核心能力

| # | 文档 | 内容 |
| --- | --- | --- |
| 03 | [Producer — 发送消息](03-Producer-发送消息.md) | `Queue::push / pushRaw`、Key 路由保序、自定义 Header、W3C Trace Context、低层 `Producer` API |
| 04 | [Consumer — 消费消息](04-Consumer-消费消息.md) | `kafka:work` 命令、批量消费（`--batch-size`）、优雅退出、低层 `Consumer` API、**`kafka:work` vs `queue:work` 性能对比**、**事务 Consumer（v0.5.4 配套，§1.6）** |
| 05 | [失败处理](05-失败处理.md) | database / dlq / hybrid 三种模式、retry 流程、`ExceptionClassRouter` 异常路由、`DlqRateLimiter` 限速 |

### 高级特性

| # | 文档 | 内容 |
| --- | --- | --- |
| 06 | [延迟消息](06-延迟消息.md) | `Queue::later` API、时间轮分层 topic、`DelayRouter` 路由规则、配置 tiers |
| 07 | [DLQ 运维](07-DLQ运维.md) | `kafka:dlq:tail` 实时打印、DLQ 消息格式解读、故障排查流程 |
| 08 | [回溯 Replay](08-回溯Replay.md) | `kafka:replay` 命令、`TimeWindowParser` 时间格式（now / -1h / Unix timestamp） |
| 09 | [Horizon 适配](09-Horizon适配.md) | `kafka:work --horizon-metrics` 启用、Lua 脚本、Redis key 格式、`/horizon` dashboard 集成 |
| 10 | [多 Connection](10-多Connection.md) | `Kafka::connection('reports')` 多集群、`KafkaManager` 缓存与断开 |
| 11 | [Serializer](11-Serializer.md) | `PhpSerializer` / `JsonSerializer` 选择、自定义序列化器实现 `Serializer` 接口 |

### 集成与测试

| # | 文档 | 内容 |
| --- | --- | --- |
| 12 | [事件系统](12-事件系统.md) | 7 个 Laravel 事件（`MessagePublishing` / `PayloadReceived` 等）、订阅方式、与失败处理协同 |
| 13 | [KafkaFake 单元测试](13-KafkaFake测试.md) | `Kafka::fake()` 启用、5 种断言 API（`assertPushed / assertPushedOn / assertPushedTimes / assertPushedOnTimes / assertNothingPushed`） |
| 14 | [安全连接](14-安全连接.md) | 4 种协议（PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL）、证书配置、SASL 机制 |

### 运维与进阶

| # | 文档 | 内容 |
| --- | --- | --- |
| 15 | [CLI 命令清单](15-CLI命令清单.md) | 4 个命令完整参数表（`kafka:work` / `kafka:dlq:tail` / `kafka:replay` / `kafka:horizon:snapshot`） |
| 16 | [高级主题](16-高级主题.md) | Octane 兼容、`disconnect()` 资源释放、常见错误排查 |
| 17 | [任务链 (Task Chain)](17-Task-Chain.md) | `Bus::chain` / `Job::withChain` 实测兼容、`$a->chain()->dispatch()` 误用陷阱、强顺序与失败回滚 |

---

## 文档阅读路径

- **刚接触本包**：01 → 02 → 03 → 04 → 05
- **想用延迟消息**：01 → 06
- **想接 Horizon 监控**：01 → 09
- **生产排障**：07 → 15
- **写单元测试**：13

---

## 代码片段约定

所有示例代码遵循以下约定：

- **命名空间**：`LaravelKafka\`（如 `LaravelKafka\Facades\Kafka`、`LaravelKafka\Producer\Message`）
- **Facade 别名**：`use LaravelKafka\Facades\Kafka;`
- **PHP 版本**：7.4 兼容（不用 `enum` / `match` / 命名参数 / nullsafe / `#[Attribute]` / 函数调用 trailing comma）
- **示例可运行**：除非特别说明，都能在 Orchestra Testbench 单元测试或真实 Laravel 项目里直接复制粘贴跑通
