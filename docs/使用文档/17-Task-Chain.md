# 17 任务链 (Task Chain)

Laravel 8 标准任务链（`Bus::chain` / `Job::withChain`）在本 Kafka 队列驱动下的实际行为、
已知陷阱、推荐写法。

---

## 1. 业务方背景

Laravel 8 任务链机制：
- `Bus::chain([A, B, C])->dispatch()` — 静态传 Job 数组
- `(new A)->withChain([B, C])->dispatch(args)` — 实例 + 链式
- `(new A)->chain([B, C])` — **API 误用**（详见 §4）

`Bus::chain` 内部把链上下一个 Job 序列化到第一个 Job 的 `data.chained` 字段，
`kafka:work` 消费完第一个 Job 后 `CallQueuedHandler::ensureNextJobInChainIsDispatched()`
自动 dispatch 链上下一个 Job。

---

## 2. 实测兼容性

probe31 e2e（业务方业务方业务方真实 Laravel 8 用法，6 条成功链 + 1 条失败链 + 1 链式 API）：

| 场景 | 用法 | 结果 |
| --- | --- | --- |
| `Bus::chain` 推 4 条 | `Bus::chain([A, B, C])->dispatch()` | ✅ A→B→C 全跑通，强顺序 |
| `Job::withChain` 推 2 条 | `Job::withChain([B, C])->dispatch([data])` | ✅ A→B→C 全跑通 |
| 失败回滚 | A 抛异常 | ✅ B/C **未被 dispatch**（CallQueuedHandler 正确处理） |
| 强顺序 | 1 partition 单 worker | ✅ A→B→C 按 ts 升序 |

**总 22/22 项全过**。详见 `laravel-test/probe31-chain.php`。

---

## 3. 强顺序保证 — 业务场景注意

| 部署模式 | chain 顺序保证 |
| --- | --- |
| 1 个 `kafka:work` 进程 + 1 partition | ✅ **保证 A→B→C 顺序** |
| 多个 `kafka:work` 进程 + 1 partition | ❌ 不同 worker 抢消息，**B 可能被另一 worker 抢走** |
| 1 个 `kafka:work` + 多 partition | ⚠️ 同 `key` 路由同 partition 才有序——但 chain step 没传 `key`，**会被轮询到任意 partition** |
| 多 worker + 多 partition | ❌❌ 双破坏：worker 抢 + partition 抢 |

### 业务场景建议

| 业务方业务方需求 | 部署建议 |
| --- | --- |
| chain 强顺序 + 失败回滚 | **单 `kafka:work` 进程 + 1 partition**（或在每个 chain 第一个 Job 显式设 `->onQueue('chain-X')` 路由到独立 topic） |
| 多 worker 并行 + chain | ❌ 改用 Redis 队列（Redis 列表 BRPOP 强顺序） |
| 多 partition 横向扩展 | 不用 chain，独立 Job 池（`orderId` 做 `key` 路由） |

---

## 4. ⚠️ Laravel 8 API 误用陷阱

业务方业务方业务方写 `$a->chain()->dispatch()` **orderId 会变 0**。这是 Laravel 8 框架
自身 API 设计，不是 Kafka 驱动 bug。

### 4.1 错误写法

```php
$a = new OrderJob(['orderId' => 10]);
$a->chain([new NotifyJob, new UpdateInventoryJob])->dispatch();
// ❌ 推到队列的是 new OrderJob() (orderId=0), 不是 $a!
```

### 4.2 根因

`Illuminate\Bus\Queueable::chain()` 实例方法：

```php
public function chain($chain) {
    $this->chained = collect($chain)->map(function ($job) {
        return $this->serializeJob($job);
    })->all();
    return $this;  // ← 返回 $this, 不是 PendingChain
}
```

链式 `->dispatch()` 调 `Illuminate\Foundation\Bus\Dispatchable::dispatch()` 静态方法：

```php
public static function dispatch(...$arguments) {
    return new PendingDispatch(new static(...$arguments));  // ← new static() 不带参数!
}
```

**结果**：`new OrderJob()` 构造时 `$data=[]` default → `$this->orderId = 0`，
原来 `$a->orderId = 10` **完全丢失**。

### 4.3 正确写法

**方案 1（推荐）**：`Job::withChain()->dispatch($args)` 静态 API，参数传给构造器：

```php
OrderJob::withChain([
    new NotifyJob(['orderId' => 10]),
    new UpdateInventoryJob(['orderId' => 10]),
])->dispatch(['orderId' => 10]);
// ✅ 推到队列的是 new OrderJob(['orderId' => 10])
```

**方案 2**：`Bus::chain()` 静态传数组：

```php
Bus::chain([
    new OrderJob(['orderId' => 10]),
    new NotifyJob(['orderId' => 10]),
    new UpdateInventoryJob(['orderId' => 10]),
])->dispatch();
// ✅ 不受 $a->chain() 误用影响
```

**方案 3**：`Bus::dispatch($a)` 直接用实例（手动）：

```php
$a = new OrderJob(['orderId' => 10]);
$a->chained = [new NotifyJob, new UpdateInventoryJob];  // 手动设
Bus::dispatch($a);
// ✅ orderId 保留, chain 走 chained 字段
```

### 4.4 验证脚本

`laravel-test/probe31f-queue-push.php` 实测：

```
offset=200 (Bus::chain API):   orderId=555  ✅
offset=201 ($a->chain API):     orderId=0    ❌ 误用!
```

---

## 5. 失败回滚实测

```php
Bus::chain([
    new OrderJob(['orderId' => 999]),
    new NotifyJob(['orderId' => 999]),
    new UpdateInventoryJob(['orderId' => 999]),
])->dispatch();
```

`OrderJob` 抛异常时：

1. `CallQueuedHandler::call()` catch 异常
2. `$job->hasFailed() = true`
3. `ensureNextJobInChainIsDispatched($command)` **不调**（callQueuedHandler.php:76 `if (! $job->hasFailed() && ! $job->isReleased())` 守卫）
4. NotifyJob / UpdateInventoryJob **未被 dispatch**
5. failed jobs 走 Laravel `queue.failer`（本包业务方配 `failed.driver = 'kafka-redis'` → RedisFailedJobProvider）

**实测结果**：probe31 #10 失败回滚 OK — orderId=999 只有 A 写 log，B/C count=0。

---

## 6. chain + Kafka 事务

| 操作 | 行为 |
| --- | --- |
| A 成功后 dispatch B | Kafka 队列 push 同步发送，**broker 已确认**后再 return |
| A 成功 + B push 失败 | B 不进 Kafka，**但 A 已 commit offset**——A 任务算成功完成 |
| A 成功 + B 延迟 push（网络抖动）| `kafka:work` 内部 retry 3 次 Producer::send |
| A 抛异常 + DLQ 模式 | A 走 DLQ topic，B/C 不 dispatch |

**注意**：Kafka **没事务**（单 partition 单消息级别）。A 完成 + B 失败不会回滚 A。
业务方业务方业务场景下 chain 是"**尽力而为**"而非"事务"——A 完成后 B 失败属于
正常可重试场景（probe31 #10 fail 模式 B/C 也不 dispatch 是**业务**层面的回滚）。

---

## 7. 总结

| 业务方业务方业务场景 | 推荐 |
| --- | --- |
| 简单 task 序列 | ✅ `Bus::chain([A, B, C])->dispatch()` |
| 静态调用 | ✅ `Job::withChain()->dispatch($args)` |
| 强顺序保证 | ⚠️ 单 `kafka:work` + 1 partition；多 worker 业务方业务方业务场景要改用 Redis |
| `$a->chain()->dispatch()` | ❌ **API 误用**，orderId 变 0，用 `Job::withChain` 替代 |
| 失败回滚语义 | ✅ Laravel CallQueuedHandler 正确处理：B/C 不 dispatch |

---

## 8. 参考

- 探针脚本：`laravel-test/probe31-chain.php`（22/22 全过）
- 实测 payload 验证：`laravel-test/probe31d-payload-detail.php`
- 失败注入 Job：`laravel-test/app/Jobs/ChainStepA.php`
- Laravel 8 CallQueuedHandler: `vendor/laravel/framework/src/Illuminate/Queue/CallQueuedHandler.php`
- Laravel 8 Dispatchable: `vendor/laravel/framework/src/Illuminate/Foundation/Bus/Dispatchable.php`
