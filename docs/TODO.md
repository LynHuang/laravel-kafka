# TODO / Technical Debt

本包所有版本待办技术债的**集中清单**。每条记录**踩坑背景** + **修法方向** + **优先级**，
未来版本（v0.5.0+）回到这包时按这个清单走。

来源：v0.4.0 release 后的多次业务方实测 + 后续维护中发现。

---

## v0.4.5 已完成 (2026-08-27)

业务方按 Laravel 8 官方队列文档（`https://learnku.com/docs/laravel/8.5/queues/10395#9a07d1`）
跑 15 项兼容性 e2e (`probe29-laravel-compat.php`) + 9 项 RedisFailedJobProvider e2e
(`probe30-redis-failed-provider.php`)，发现 2 个 P0 兼容 bug，v0.4.5 修：

1. **KafkaQueue::size() 真实实现** —— 之前 v0.1 占位 `return 0`，致 `queue:monitor` /
   `queue:size` 看到 0。改用 `RdKafka\Consumer` + `queryWatermarkOffsets` 算物理 topic 消息总数。
2. **新增 RedisFailedJobProvider** —— 业务方业务环境无 pdo_sqlite / MySQL 时 `queue:failed` /
   `queue:retry` / `queue:forget` / `queue:flush` 完全可用，业务方配 `failed.driver = 'kafka-redis'`
   即可。

15/15 + 9/9 全过，0 regression。详见 `docs/CHANGELOG.md` v0.4.5 节。

---

## v0.4.6 已完成 (2026-08-27)

v0.4.5 release 后业务方跑 e2e probe32 (`queue:work` 真消费) + probe33 (Horizon snapshot
路径) 发现 2 个 P0 兼容 bug，v0.4.6 修：

1. **KafkaQueue::pop() 真实实现** —— v0.1 占位返 null，业务方跑
   `queue:work --once --connection=kafka` 拿到 null 立即退出，**消费不到任何消息**。
   实现 1000ms 阻塞 poll + 包装 KafkaJob 给 Worker。

   **Trade-off**（业务方已知）：消息延迟从 1ms (`kafka:work` 真长轮询) 退化到 5s
   (`queue:work` 默认 `--sleep=3s` + pop 1s + 下一轮 1s)。业务方接受此退化才能用
   `queue:work` 命令——本包推荐仍用 `kafka:work`（高性能 + 1ms 延迟）。

   验证：`laravel-test/probe32-queue-work.php` 9/9 全过。

2. **HorizonSnapshot::snapshotJob() 新增** —— v0.4.0-0.4.5 Command 处理 `measured_jobs` 错调
   `snapshotQueue` 写到 `snapshot:queue:<class>`，不是 Horizon 期望的 `snapshot:job:<class>`。
   新增独立方法，Command 区分 queue/job 调不同方法。`snapshotQueue()` 行为不变（向后兼容）。

   验证：`laravel-test/probe33-horizon-snapshot-job.php` 9/9 全过。

9/9 + 9/9 全过，0 regression（v0.4.3 已知 phpunit fail 仍 continue-on-error）。
详见 `docs/CHANGELOG.md` v0.4.6 节。

---

## v0.4.7 已完成 (2026-08-27)

v0.4.6 release 后跑 phpunit 137/287 全过（之前 `KafkaQueueFakeTest::testPushRawInNormalModeGoesToRealProducer` 在 CI fail），删 `.github/workflows/tests.yml` 里 v0.4.3 hotfix 加的 `continue-on-error: true`。

1. **`KafkaQueueFakeTest::testPushRawInNormalModeGoesToRealProducer` 核心断言改写**
   - v0.4.3-0.4.6 假设 `non-fake 模式下 pushRaw 必抛 Throwable`（因为没真 Kafka 集群），但 CI runner 有 `services.kafka` (KRaft 单 broker) + 业务方本机有 Kafka，**真发成功不抛**，test fail。
   - 改核心断言：从"无 Kafka 抛异常" → **"non-fake 模式不写 FakeMessageStorage"**（无论真发成功/抛 KafkaException）。
   - 接受两种环境（有 Kafka 断言 return isInt，无 Kafka 用 addToAssertionCount 接受异常）。
   - 关键断言 `$this->assertSame(0, $storage->count())` 保持不变。

2. **CI tests.yml 删 `continue-on-error: true`**
   - `tests.yml` Run tests step 删 v0.4.3 hotfix 加的 `continue-on-error: true` + 更新注释。
   - **linter.yml 保留** `continue-on-error: true`：phpstan 120 errors + cs-fixer 40 文件修复仍是历史债，v0.4.7 不修（工作量太大），留到 v0.4.8。

0 regression。详见 `docs/CHANGELOG.md` v0.4.7 节。

---

## v0.4.8 已完成 (2026-08-27)

v0.4.7 release 后跑 phpstan level 6 报 120 errors + cs-fixer 报 40 文件 diff，v0.4.8 **全部清理**：

### 1. PHPStan 120 errors → 0 errors

| 类别 | 数量 | 修法 |
| --- | --- | --- |
| **Dynamic property 缺失** | 70+ | 7 个值对象类（`KafkaConfig` / `Message` / `Events/*` 6 个 / `FailedContext`）加显式 `private` property 声明（PHP 8.2+ 推荐做法）|
| **Illuminate Container 类型 mismatch** | 4 | `KafkaJob` / `KafkaQueue::setContainer` 改用具体 `Illuminate\Container\Container`（与父类对齐）|
| **`createPayload` 4 参数 vs 父类 3 参数** | 3 | 修 v0.1 老 bug：去掉错传 `$this->connectionName`（connection 名不是 queue 名）|
| **Never read / unreachable** | 5 | `$serializer` 字段 + HybridFailedJobHandler 2 个 truncate 字段保留并加 `@phpstan-ignore-next-line`（Factory 注入兼容）；Producer `$lastDeliverySucceeded` 死代码加 ignore（librdkafka async callback）|
| **Laravel 8 Connection magic method** | 13 | `RedisFailedJobProvider` 改 `redis(): Connection` 强类型 + phpstan.neon 加 `ignoreErrors` 项目级规则 |

### 2. PHP-CS-Fixer 40 文件 → 0 diff

`vendor/bin/php-cs-fixer fix` 自动应用：`$this->assert* → self::assert*`、删 unused imports、ordered_class_elements 重排。

### 3. CI workflows 删 `continue-on-error: true`

- `linter.yml` phpstan + cs-fixer step 删 v0.4.1 hotfix 加的 `continue-on-error: true`（现在 0 errors + 0 diff）
- `tests.yml` Redis service block 加（v0.4.7 之前 CI 没 Redis，Horizon 集成测不了）
- `linter.yml` + `tests.yml` 注释更新

### 4. v0.4.7 #3 chain API 防御决定跳过

`$a->chain()->dispatch()` 误用是 Laravel 8 框架 API 缺陷（`Bus\Queueable::chain()` 返回 `$this` + `Dispatchable::dispatch()` static `new static()` 不带参数），源码层防御代价 > 收益。v0.4.5 文档化已充分（`docs/使用文档/17-Task-Chain.md`）。

### 5. v0.4.7 #4 librdkafka 1.6.2 升级决定跳过

`probe32b-rdkafka-version.php` 实测：业务方业务环境 ext-rdkafka 6.0.1 = **librdkafka 2.5.0**（不是 1.6.2）。之前"1.6.2 commit 60s timeout"是**单 broker + IPv4/IPv6 切换**环境问题，不是版本问题。

### 6. v0.4.7 #5 JsonSerializer 测试已覆盖（无需新增）

`tests/Unit/Producer/Serializer/JsonSerializerTest.php` 已有 6 个 test（encode/decode/unicode/slashes/empty/invalid/name），6/6 全过。v0.4.7 TODO 说"没覆盖"是文档错误。

### 7. v0.4.7 #6 CI Redis service 已加

`tests.yml` services 加 `redis:7.2-alpine` + health check。

**0 regression**。phpunit 137/287 + phpstan 0 + cs-fixer 0 + probe29 15/15 + probe30 9/9 全过。详见 `docs/CHANGELOG.md` v0.4.8 节。

---

## v0.5.0 已完成 (2026-08-27)

**Serializer 真正接入队列管道**（v0.2 设计但一直没实现，probe34/probe35 实测发现是死代码）：

1. **`NativeHandler` 支持裸事件（非 Laravel Job）+ Serializer 反序列化**
   - `handle()` 加裸事件检测（payload 无 `data.command`）
   - `handleRawPayload()`：按 `x-serializer` header resolve Serializer → decode → dispatch `PayloadReceived` → ack
   - `registerSerializer()` 公共 API（兑现文档 §4 承诺）+ `resolveSerializer()` 懒加载 registry（php/json）
   - 解码失败 → 复用 `onException`（requeue/dlq 与 Laravel Job 一致）
   - **新事件** `src/Events/PayloadReceived.php`（topic + decoded payload + 原始 Message）
   - Laravel Job 路径（`Queue::push`/`dispatch` → Worker::process）完全不变，不误触 PayloadReceived

2. **跨语言消费能力**：裸事件 + JsonSerializer 真实可用，Node/Go/Python 可直接 json.loads 读裸事件

3. **重要限制**：Laravel Job 的 `data.command` 是 PHP serialize（Laravel 框架内部格式），
   跨语言消费**必须用裸事件而非 Laravel Job**（文档 §3 已说明）

**验证**：probe36 7/7（JSON 裸事件 → decode → PayloadReceived 含中文 → ack）+ Laravel Job 不误触。
phpunit 137/287 + phpstan 0 + cs-fixer 0。详见 `docs/CHANGELOG.md` v0.5.0 节。

---

## 长期 backlog（v0.5.1+ 考虑）

### 1. 业务方 laravel-test 项目的清理

业务方测试项目 `laravel-test/` 在 `vendor/lyn-huang/laravel-kafka/` 是 git archive 快照
+ 手动 Copy-Item 同步 hotfix。**业务方业务环境** `composer require` 装会丢这些 hotfix。

**修法**（v0.5.1）：
- 业务方 release 后跑 `composer update lyn-huang/laravel-kafka` 重打 vendor
- 或 git clone 源码 + 手动 `cp -r src/ vendor/lyn-huang/laravel-kafka/src/`
- README 加 "Business Testing" 章节

---

### 2. 单元测试用 Testbench + 真 Redis（integration test 改造）

v0.4.0 unit test 默认不连 Kafka，**但 Horizon 集成测试需要真 Redis**。v0.4.8 在 CI services
加了 redis:7.2-alpine + health check, 但 unit test 套件还没用 Testbench 起 Redis connection.

**修法**（v0.5.1）：
- `tests/Integration/Horizon/` 加 e2e 测试：`KafkaQueueFake` push → `kafka:work` 消费
  → 调 `recordHorizonMetrics` → 验证 Redis 写入
- 这样 CI 也能跑 Horizon 集成（已有 Redis services）

---

## 跟踪规则

1. v0.5.0 → v0.5.1：考虑把 #1 laravel-test 清理 + #2 Horizon integration test 一起做
2. 每次修完从本文件删对应条目，并在 `docs/CHANGELOG.md` 新版本里写 "Fixed" 链接回本文件
3. v0.5.0 release 后本文件**只剩 backlog**（无 high/medium 优先级）
