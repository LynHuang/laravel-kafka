# 08 回溯 Replay

`kafka:replay` 命令 + `TimeWindowParser` 时间格式 + 实际 reproduce（v0.3 MVP 状态）。

---

## 1. `kafka:replay` 命令

```bash
php artisan kafka:replay [options]
```

### 选项

| 选项 | 必填 | 说明 |
| --- | --- | --- |
| `--topic=` | ✅ | 源 topic（要重放） |
| `--from=` | ✅ | 起始时间（见 §3） |
| `--to=` | ✅ | 结束时间（见 §3） |
| `--target-topic=` | ✅ | 目标 topic（重放到哪里） |
| `--group=replay-runner` | — | 独立 consumer group（不影响主消费者） |

### 示例

```bash
# 把 orders.events 过去 1 小时的消息重新推到 orders.events.replay
php artisan kafka:replay \
    --topic=orders.events \
    --from="-1h" \
    --to=now \
    --target-topic=orders.events.replay \
    --group=replay-runner
```

输出：

```
[kafka:replay] topic=orders.events window=[-1h (1700000000) → now (1700003600)] target=orders.events.replay group=replay-runner
[kafka:replay] v0.3 MVP: window validated, actual reproduce not implemented yet (v0.4 评估)
```

---

## 2. v0.3 MVP 限制

> **当前 v0.5.2 版本**：`kafka:replay` 只做**时间窗口解析 + 参数校验**，**不**实际 reproduce 消息
> （`ReplayCommand.php:80` warn "actual reproduce not implemented yet"）。

实际 reproduce（调 `librdkafka offsetsForTimes` 找 offset，再遍历 partition 重放）在路线图 v0.6，尚未实现。

### 业务方临时方案

业务方需要立即重放时，用 `kcat` + 自定义脚本：

```bash
# 1. 用 kcat 把指定时间窗口的消息 dump 出来
kcat -b localhost:9092 -t orders.events \
    -C -o stored \
    -e -q \
    -f '\n--- offset=%o partition=%p ts=%T key=%k ---\n%s\n' \
    > /tmp/orders-snapshot.txt

# 2. 过滤时间窗口（用 awk）
awk -v from="$FROM_TS" -v to="$TO_TS" '
    /^--- offset=/ {
        ts = $0; sub(/.*ts=([0-9]+).*/, "\\1", ts);
        if (ts >= from && ts <= to) keep = 1;
        else keep = 0;
    }
    keep
' /tmp/orders-snapshot.txt > /tmp/orders-window.txt

# 3. 用 Python 脚本 reparse + produce 到目标 topic
python3 replay.py --input /tmp/orders-window.txt --target orders.events.replay
```

> `kcat` 跨语言工具在 §4 详细讲。

---

## 3. 时间窗口格式

`TimeWindowParser` 支持 4 种格式：

| 输入 | 含义 | 例子 |
| --- | --- | --- |
| `now` | 当前时间（`time()`） | `--from=now` |
| `-1h` / `-30m` / `-7d` / `-60s` | 相对时间偏移 | `--from=-1h` = 1 小时前 |
| `1700000000` | 绝对 Unix timestamp | `--from=1700000000` |
| `2026-08-25 10:00:00` | 绝对时间字符串（`strtotime`） | `--from="2026-08-25 10:00:00"` |

### 单位

| 后缀 | 含义 | 换算 |
| --- | --- | --- |
| `s` | 秒 | 1 |
| `m` | 分钟 | 60s |
| `h` | 小时 | 3600s |
| `d` | 天 | 86400s |

### 校验

- `from` 必须早于 `to`（业务方写反会失败）
- 无效格式抛 `LaravelKafka\Exceptions\KafkaException`
- 空字符串抛异常

### `TimeWindowParser` 低层 API

```php
use LaravelKafka\Replay\TimeWindowParser;

$parser = new TimeWindowParser();

$parser->parse('now');                  // 1700000000 (当前)
$parser->parse('-1h');                  // 1699996400 (1 小时前)
$parser->parse('1700000000');           // 1700000000
$parser->parse('2026-08-25 10:00:00');   // strtotime 解析
```

### 注入测试

```php
// 测试时注入固定时间
$parser->parse('-1h', $now = 1700000000);  // 1699996400
```

---

## 4. 用 `kcat` 做完整重放（临时方案）

[kcat](https://github.com/edenhill/kcat) 是 librdkafka 官方命令行工具。

### 安装

```bash
# macOS
brew install kcat

# Ubuntu
apt-get install kafkacat
```

### 按时间窗口 dump

```bash
# 全量 dump（含 key + headers + ts）
kcat -b localhost:9092 -t orders.events \
    -C -o stored -e \
    -f 'key=%k\nts=%T\noffset=%o partition=%p\nheaders=%h\npayload:\n%s\n---\n' \
    > /tmp/snapshot.txt
```

### 按时间过滤

```bash
# 假设我们要重放 2026-08-25 10:00:00 到 2026-08-25 11:00:00
FROM_TS=$(date -d "2026-08-25 10:00:00" +%s)000
TO_TS=$(date -d "2026-08-25 11:00:00" +%s)000

awk -v from="$FROM_TS" -v to="$TO_TS" '
    /^key=/ { current_key = $0 }
    /^ts=/ {
        ts = $0
        sub(/^ts=/, "", ts)
        if (ts+0 >= from && ts+0 <= to) in_range = 1
        else in_range = 0
    }
    in_range
' /tmp/snapshot.txt > /tmp/window.txt
```

### 写回目标 topic

业务方写个简单脚本读取 `/tmp/window.txt` 逐条 reparse + produce：

```python
#!/usr/bin/env python3
import re
from kafka import KafkaProducer
import sys

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open(sys.argv[1]) as f:
    current = {}
    for line in f:
        line = line.rstrip()
        if line.startswith('key='):
            current['key'] = line[4:].encode()
        elif line.startswith('ts='):
            current['ts'] = int(line[3:])
        elif line.startswith('offset='):
            m = re.match(r'offset=(\d+) partition=(\d+)', line)
            current['offset'] = int(m.group(1))
            current['partition'] = int(m.group(2))
        elif line.startswith('headers='):
            current['headers'] = []
            for h in line[8:].split(','):
                if h:
                    k, v = h.split(':', 1)
                    current['headers'].append((k.encode(), v.encode()))
        elif line.startswith('payload:'):
            current['payload'] = b''
        elif line == '---':
            if current.get('payload') is not None:
                producer.send(
                    'orders.events.replay',
                    key=current.get('key'),
                    value=current['payload'],
                    headers=current.get('headers', []),
                )
            current = {}
        else:
            if 'payload' in current:
                current['payload'] += (line + '\n').encode()

producer.flush()
print("Replayed all messages")
```

---

## 5. 配置

```php
// config/kafka.php
'connections' => [
    'default' => [
        'replay' => [
            'preserve_partition' => true,   // 重放时保持原 partition（保序）
        ],
    ],
],
```

> **v0.5.2 修正**：`preserve_partition` 配置键存在但**无代码消费**（replay 未实现）。
> 以下是**设计意图**，等 v0.6 reproduce 实现后生效：
> `preserve_partition=true` 时用原 `key` 作 partition 路由键（同 key 落同 partition）；
> `preserve_partition=false` 时 librdkafka 轮询 partition（不保序但更均衡）。

---

## 6. 完整示例：业务方重放脚本

业务方要重放 `laravel-jobs` 过去 2 小时失败的消息（"双 11 大促后回滚"）：

```bash
# Step 1：算时间戳
FROM_TS=$(date -d "-2 hours" +%s)
TO_TS=$(date +%s)

# Step 2：用 kcat dump
kcat -b localhost:9092 -t laravel-jobs \
    -C -o stored -e \
    -f 'key=%k\noffset=%o partition=%p\nts=%T\nheaders=%h\npayload:\n%s\n---\n' \
    > /tmp/laravel-jobs-snapshot.txt

# Step 3：过滤窗口
awk -v from="${FROM_TS}000" -v to="${TO_TS}000" '
    /^ts=/ { ts = $0; sub(/^ts=/, "", ts); in_range = (ts+0 >= from && ts+0 <= to) ? 1 : 0 }
    in_range
' /tmp/laravel-jobs-snapshot.txt > /tmp/laravel-jobs-window.txt

# Step 4：写回（用 Python 脚本）
python3 /usr/local/bin/laravel-replay.py /tmp/laravel-jobs-window.txt laravel-jobs

# Step 5：起 worker 消费
php artisan kafka:work --queue=laravel-jobs
```

---

## 7. 适用场景

- **业务回滚**：误操作后重放指定时间窗口
- **数据修复**：bug 修复后重放历史数据让新逻辑生效
- **灾备演练**：把生产流量重放到预发环境
- **A/B 测试**：把生产数据重放到测试 topic 给实验消费者分析

---

## 8. 下一步

- Horizon 适配：[09-Horizon适配](09-Horizon适配.md)
- 多 Connection：[10-多Connection](10-多Connection.md)
