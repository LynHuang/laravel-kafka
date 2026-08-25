# laravel-kafka

[![PHP](https://img.shields.io/badge/PHP-7.4%2B-777bb4)](https://www.php.net)
[![Laravel](https://img.shields.io/badge/Laravel-8%20%7C%209%20%7C%2010%20%7C%2011-ff2d20)](https://laravel.com)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![CI](https://img.shields.io/badge/CI-7.4%20%7C%208.1%20%7C%208.3-blue)](.github/workflows/tests.yml)

Apache Kafka Queue driver & event bus for Laravel. **PHP 7.4+**、强依赖 `ext-rdkafka`。

> 📦 包名：`lyn-huang/laravel-kafka`
> 📚 设计文档：[`开发文档_v0.1.md`](开发文档_v0.1.md)
> 📓 实施日志：[`docs/开发日志_v0.1.md`](docs/开发日志_v0.1.md)
> 📝 变更日志：[`docs/CHANGELOG.md`](docs/CHANGELOG.md)

---

## 快速开始

```bash
# 1. 安装（v0.1 暂不发布 Packagist，需在项目 composer.json 配仓库源）
composer require lyn-huang/laravel-kafka:dev-main
```

`config/queue.php` 中加入：

```php
'connections' => [
    'kafka' => [
        'driver'  => 'kafka',
        'name'    => 'default',
        'brokers' => env('KAFKA_BROKERS', 'localhost:9092'),
        'queue'   => env('KAFKA_DEFAULT_TOPIC', 'laravel-jobs'),
    ],
],

'default' => env('QUEUE_CONNECTION', 'kafka'),
```

`.env`：

```dotenv
QUEUE_CONNECTION=kafka
KAFKA_BROKERS=localhost:9092
KAFKA_DEFAULT_TOPIC=laravel-jobs
KAFKA_GROUP_ID=laravel-default
KAFKA_FAILED_DRIVER=hybrid
```

启动 worker：

```bash
php artisan kafka:work --queue=laravel-jobs
```

---

## 特性矩阵

| 能力 | v0.1 | v0.2 | v0.3 | v0.4 |
| --- | :-: | :-: | :-: | :-: |
| Laravel Queue 标准驱动 | ✅ | ✅ | ✅ | ✅ |
| 失败处理（database/dlq/hybrid） | ✅ | ✅ | ✅ | ✅ |
| Consumer Group 水平扩展 | ✅ | ✅ | ✅ | ✅ |
| 多 Topic 路由 | — | ✅ | ✅ | ✅ |
| Key Routing 顺序保证 | — | ✅ | ✅ | ✅ |
| DLQ 独立 topic | — | ✅ | ✅ | ✅ |
| 延迟消息（时间轮） | — | ✅ | ✅ | ✅ |
| Header Trace / 透传 | — | ✅ | ✅ | ✅ |
| 批量消费 | — | ✅ | ✅ | ✅ |
| 回溯 Replay | — | ✅ | ✅ | ✅ |
| 事务 + 幂等 | — | — | ✅ | ✅ |
| 多 Consumer Group Fan-out | — | — | ✅ | ✅ |
| Schema Registry | — | — | — | ✅ |
| Horizon 适配 | — | — | — | ✅ |

---

## 与 Laravel Redis 队列对比

| 维度 | Redis 队列 | laravel-kafka |
| --- | --- | --- |
| 持久化 | 内存 + AOF/RDB | 磁盘顺序写 |
| 水平扩展 | 共享 Redis 即可 | Consumer Group + 分区 |
| 严格顺序 | 无 | 同分区有序（按 key） |
| 死信 | `failed_jobs` 表 | 独立 DLQ topic |
| 延迟队列 | `delay_seconds` 字段，worker sleep | 时间轮分层 topic（v0.2） |
| 回溯 | 不可 | 按 offset / 时间戳回放（v0.2） |
| 跨语言消费 | 受限 | Kafka 协议天然多语言 |
| 运维成本 | 低 | 中（需维护 Kafka 集群） |

---

## 系统要求

- **PHP 7.4+**（8.0+ 享受 Carbon 3 与 Laravel 9+；7.4 仅 Laravel 8）
- **`ext-rdkafka`**（`pecl install rdkafka`，需要 librdkafka ≥ 1.5）
- **Laravel 8 / 9 / 10 / 11**
- **Kafka 0.11+**（KRaft 单节点或集群均可）

### 安装 ext-rdkafka

```bash
# macOS
brew install librdkafka && pecl install rdkafka

# Ubuntu / Debian
apt-get install librdkafka-dev && pecl install rdkafka

# 验证
php -m | grep rdkafka
```

---

## 常见问题

**Q：如何让消费按顺序执行？**
A：v0.2 起 `Kafka::send($topic, $msg, key: 'user-42')`，同 key 永远落同分区。

**Q：失败任务怎么排查？**
A：两种方式：
- `database` / `hybrid` 模式：`php artisan queue:failed` 列表，`queue:retry uuid` 重试
- `dlq` / `hybrid` 模式：消费 `<topic>.dlq` 独立排查

**Q：能从历史时间点重放任务吗？**
A：v0.2 起 `php artisan kafka:replay --topic=x --from=-1h`。

**Q：能多个业务方独立消费同一份消息吗？**
A：v0.3 起多 Consumer Group Fan-out 支持。

---

## 路线图

- **v0.1（当前）**：Laravel 队列可替换 + 三模式失败处理
- **v0.2**：多 Topic 路由 / Key Routing / DLQ 高级特性 / 延迟 / Trace / 批量 / Replay
- **v0.3**：事务 / 幂等 / 多 Consumer Group / Read-Committed
- **v0.4**：Schema Registry / OpenTelemetry / Horizon（重新评估）
- **v0.5**：性能压测 / 评估转公开

---

## 贡献

v0.1 仓库**暂为私有**，仅接受显式邀请的协作者。详细的开发规范见 [`开发文档_v0.1.md` §13](开发文档_v0.1.md#13-php-74-编码规范v01-强制约束) 与 §14 RFC 记录。

---

## License

[MIT](LICENSE) © 2026 Lyn-Huang
