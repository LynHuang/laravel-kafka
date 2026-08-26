# 09 Horizon 适配

`kafka:work --horizon-metrics` 启用 + Lua 脚本 + Redis key 格式 + Horizon dashboard 集成。

---

## 1. 概念

把 `kafka:work` 处理的消息 metrics 写到 **Horizon 5.x 兼容格式**的 Redis，业务方在 `/horizon` dashboard 直接看到 Kafka 队列的 throughput / runtime。

### 复用 Horizon Lua 脚本

直接复制 `Laravel\Horizon\LuaScripts::updateMetrics()`，**完全兼容** Horizon 数据格式：

```lua
redis.call('hsetnx', KEYS[1], 'throughput', 0)
redis.call('sadd', KEYS[2], KEYS[1])
local hash = redis.call('hmget', KEYS[1], 'throughput', 'runtime')
local throughput = hash[1] + 1
local runtime = 0
if hash[2] then
    runtime = ((hash[1] * tonumber(hash[2])) + tonumber(ARGV[1])) / throughput
else
    runtime = tonumber(ARGV[1])
end
redis.call('hmset', KEYS[1], 'throughput', throughput, 'runtime', runtime)
```

### 业务方效果

- Horizon dashboard `/horizon` 自动多一组"队列"显示
- 队列名来自 `x-queue` header（与 Laravel Queue 语义一致）
- metrics：throughput（处理总数）+ runtime（平均处理时间，ms）

---

## 2. 启用

### Step 1：装 Horizon

```bash
composer require laravel/horizon illuminate/redis predis/predis
```

发布 Horizon 资源：

```bash
php artisan vendor:publish --provider="Laravel\Horizon\HorizonServiceProvider"
```

### Step 2：配 `config/database.php` 的 `horizon` connection

```php
'redis' => [
    'client' => env('REDIS_CLIENT', 'phpredis'),
    'horizon' => [
        'host' => env('REDIS_HOST', '127.0.0.1'),
        'password' => env('REDIS_PASSWORD'),
        'port' => env('REDIS_PORT', '6379'),
        'database' => env('REDIS_DB', '0'),
    ],
],
```

> `horizon` connection 名默认与 `--horizon-redis` 选项一致。

### Step 3：启动 worker 启用 metrics

```bash
php artisan kafka:work \
    --queue=laravel-jobs \
    --horizon-metrics \
    --horizon-redis=horizon \
    --horizon-prefix=horizon:
```

输出：

```
[kafka:work] Horizon metrics enabled (connection=horizon, prefix=horizon:)
```

---

## 3. 命令行选项

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `--horizon-metrics` | `false` | 启用 |
| `--horizon-prefix` | `horizon:` | Redis key 前缀 |
| `--horizon-redis` | `horizon` | Redis connection 名 |

---

## 4. Redis key 格式

| Key | 类型 | 内容 | 来源（Lua 脚本） |
| --- | --- | --- | --- |
| `<prefix>measured_queues` | Set | queue 名列表（带 `queue:` 前缀） | `sadd` |
| `<prefix>queue:<queueName>` | Hash | `{throughput: int, runtime: float}` | `hmset` |
| `<prefix>measured_jobs` | Set | job 类名列表（带 `job:` 前缀） | `sadd` |
| `<prefix>job:<className>` | Hash | `{throughput: int, runtime: float}` | `hmset` |
| `<prefix>snapshot:queue:<queueName>` | Sorted Set | 历史快照（业务方调 `kafka:horizon:snapshot` 写入） | 业务命令 |
| `<prefix>last_snapshot_at` | String | 最后 snapshot timestamp | 业务命令 |

`<prefix>` 默认 `horizon:`（与 Horizon config 一致，业务方可自定义）。

### 示例（默认 prefix）

```
horizon:measured_queues                     → {"queue:laravel-jobs"}
horizon:queue:laravel-jobs                  → {throughput: 1234, runtime: 5.6}
horizon:measured_jobs                       → {"job:App\\Jobs\\SendOrderEmail"}
horizon:job:App\\Jobs\\SendOrderEmail       → {throughput: 1000, runtime: 4.2}
```

### runtime 计算

加权平均：

```lua
runtime_new = ((throughput_old * runtime_old) + current_runtime_ms) / (throughput_old + 1)
```

---

## 5. `kafka:horizon:snapshot` 命令

> 模板化命令，业务方通常用 Horizon 自带的 `horizon:snapshot`（更完整）。

```bash
php artisan kafka:horizon:snapshot \
    --connection=horizon \
    --prefix=horizon: \
    --trim=24 \
    --trim-job=24
```

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `--connection` | `horizon` | Redis connection 名 |
| `--prefix` | `horizon:` | Redis key 前缀 |
| `--trim` | `24` | 每个 queue 保留的快照数（Sorted Set 长度） |
| `--trim-job` | `24` | 每个 job 保留的快照数 |

### 加到 scheduler

```php
// app/Console/Kernel.php
protected function schedule(Schedule $schedule)
{
    // 每分钟 snapshot
    $schedule->command('horizon:snapshot')->everyMinute();

    // 或者用本包命令（不依赖 Horizon）
    $schedule->command('kafka:horizon:snapshot')->everyMinute();
}
```

> 推荐用 Horizon 自带命令（业务方已装 Horizon 时）—— 业务方不需要再写一份。

---

## 6. 业务方完整示例

```bash
# 启动 horizon（dashboard）
php artisan horizon

# 启动 kafka worker（带 metrics）
php artisan kafka:work --queue=laravel-jobs --horizon-metrics

# 访问 dashboard
open http://your-app.test/horizon
```

dashboard 会显示：

```
Queue: laravel-jobs
  Throughput: 1,234 / min
  Avg Runtime: 5.6 ms
  
Job: App\Jobs\SendOrderEmail
  Processed: 1,000 / min
  Avg Runtime: 4.2 ms
```

---

## 7. 故障排查

### metrics 没写到 Redis

检查：

1. `php -m | grep redis` — phpredis 扩展
2. `composer show | grep redis` — predis/predis 或 phpredis
3. `php artisan tinker` 试 `Redis::connection('horizon')->set('test', '1')` 是否成功
4. `--horizon-metrics` 真的传了？（`kafka:work` 启动日志会打印"Horizon metrics enabled"）

### Redis 报错

worker 默认不抛 metrics 异常（避免 metrics 失败影响主流程），但会有 `error_log` 输出：

```
[laravel-kafka] Horizon metrics failed: <Redis error>
```

---

## 8. 业务方自定义 metrics prefix

如果业务方有多个 Laravel 项目共享 Redis：

```bash
# project-a
php artisan kafka:work --horizon-prefix=horizon:project-a:

# project-b
php artisan kafka:work --horizon-prefix=horizon:project-b:
```

但同一 dashboard 只能看一个 prefix —— 业务方要多 dashboard 就跑多份 Horizon。

---

## 下一步

- 多 Connection：[10-多Connection](10-多Connection.md)
- Serializer：[11-Serializer](11-Serializer.md)
