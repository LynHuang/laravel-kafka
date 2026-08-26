# 性能基准 (v0.3 Step 5)

## 概述

3 个独立 benchmark 脚本，量化 laravel-kafka 的核心能力：

| 脚本 | 测什么 | 关键指标 |
| --- | --- | --- |
| `produce-throughput.php` | 单 producer 极限发送速率 | msg/s + MB/s |
| `consume-throughput.php` | 单 consumer 极限消费速率 | msg/s + 端到端延迟 p50/p95/p99 |
| `latency-p50-p99.php` | produce → consume 端到端延迟分布 | p50/p90/p95/p99/min/max |

## 用法

### 1. 启动 Kafka broker

```bash
# docker-compose
docker run -d --name kafka-bench -p 9092:9092 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    apache/kafka:3.7.0
```

### 2. 跑 produce 基准

```bash
php benchmarks/produce-throughput.php 10000 1024 bench-prod
```

输出：
```
=== Produce Throughput Benchmark ===
messages:    10000
payload:     1024 bytes
topic:       bench-prod
brokers:     localhost:9092

=== Results ===
Duration:        1.234 s
Messages:        10000
Throughput:      8102 msg/s
Bandwidth:       7.91 MB/s
Memory delta:    2.34 MB
```

### 3. 跑 consume 基准

```bash
php benchmarks/consume-throughput.php 10000 bench-prod 30
```

输出：
```
=== Consume Throughput Benchmark ===
expected:   10000
topic:      bench-prod
timeout:    30 s
brokers:    localhost:9092

=== Results ===
Duration:    0.876 s
Consumed:    10000 / 10000
Throughput:  11415 msg/s

--- End-to-end latency (ms) ---
p50:  1.23
p95:  3.45
p99:  7.89
max:  12.34
```

### 4. 跑延迟基准

```bash
php benchmarks/latency-p50-p99.php 1000 bench-latency
```

## v0.3 实测基线（参考）

跑在 v0.3 之前的某个 Linux 容器（4C8G）：

| 指标 | 值 |
| --- | --- |
| Produce 吞吐（1KB 消息，acks=all，idempotence） | ~8000 msg/s |
| Consume 吞吐（单 partition，commitAsync） | ~11000 msg/s |
| 端到端延迟 p50 | ~1-2 ms |
| 端到端延迟 p99 | ~5-10 ms |

## v0.3 优化目标

| 优化点 | 期望提升 |
| --- | --- |
| **批量消费** (`--batch-size=50`) | Consume 吞吐 +30% |
| **时间轮分层延迟** | 延迟消息 worker 不再被 sleep 阻塞 |
| **整批 commit** | 减少 commit 开销（一次 commit 替代 N 次） |

## 环境变量

| 变量 | 默认 | 用途 |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | broker 地址 |

## 注意事项

- 性能结果依赖**机器配置**（CPU / 内存 / 网络 / 磁盘 IOPS）
- 单 broker 比多 broker 慢（没有并行 partition）
- 实际生产环境按业务 SLA 参考（不要盲信单机器基准）

## vs mateusjunges 对比（计划）

v0.3 完成后，跑 `benchmarks/compare-mateusjunges.php`（v0.4 评估）对比：
- 同样硬件 + 同样数据 → 量化差距
- 重点对比：吞吐 / 延迟 / 内存占用
