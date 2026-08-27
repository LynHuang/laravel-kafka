# TODO / Technical Debt

本包所有版本待办技术债的**集中清单**。每条记录**踩坑背景** + **修法方向** + **优先级**，
未来版本（v0.4.8+）回到这包时按这个清单走。

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

## v0.4.7 → v0.4.8 待办

### 1. phpstan 历史 120 errors（中优先级，linter continue-on-error 已加）

**踩坑**：v0.4.1 时 PHPStan level 6 报 120 errors，分 4 类：

- **Dynamic property 缺失**（70+ errors）：`KafkaConfig` / `Message` / `Events/*` / `FailedContext` 等类在 `__construct` 赋值但**没显式 `private` property 声明**（PHP 8.2+ 默认禁止 dynamic property）。
- **Illuminate 类型 mismatch**（4 errors）：`Illuminate\Container\Container` vs `Illuminate\Contracts\Container\Container`。
- **`createPayload` 参数**（3 errors）：4 参数调用但父类只接受 2-3 个。
- **Never read / unreachable**（5 errors）：写不读 property + 死代码分支。

**现状**：`.github/workflows/linter.yml` phpstan step 有 `continue-on-error: true`。

**修法**：
- 修业务代码 120 errors（`KafkaConfig` 等加显式 property 声明，删 unused imports / properties，修 `createPayload` 参数兼容）
- 删 workflow 里的 `continue-on-error: true`
- 不降 phpstan ruleset

**优先级**：中（CI 实际能卡 linter 是好事，但 120 errors 是历史债 + 修起来工作量大）

---

### 2. PHP-CS-Fixer 40 文件风格修复（低优先级，linter continue-on-error 已加）

**踩坑**：v0.4.1 round 7 时 cs-fixer 3.95 在 PHP 8.1 对 40 个文件提 diff（`$this->assert* → self::assert*`、删 unused imports、ordered_class_elements 重排等）。PHP 7.4 上都合法，但 cs-fixer --dry-run 任何待修即 exit 8。

**现状**：`.github/workflows/linter.yml` cs-fixer step 有 `continue-on-error: true`。

**修法**：跑 `vendor/bin/php-cs-fixer fix` 自动应用修复，commit 改动，删 `continue-on-error`。

**优先级**：低（风格修复不影响功能）。

---

### 3. chain API 误用陷阱源码层防御（中优先级，决定跳过）

**踩坑**（probe31 实测发现）：业务方写 `$a->chain([B, C])->dispatch()` 推 orderId=0 到 queue。

**根因**：
- `Illuminate\Bus\Queueable::chain()` 实例方法返回 **`$this`**（不是 PendingChain）
- 链式 `->dispatch()` 调 `Dispatchable::dispatch()` **static** 方法 → `new static()` **不带参数**
- 推到 queue 的是 `new OrderJob()`（orderId=0），**不是** `$a`！

**修法**：
- ✅ **已完成（v0.4.5）**：写 `docs/使用文档/17-Task-Chain.md` 详细说明 + 3 种正确写法（`Job::withChain` / `Bus::chain` / `Bus::dispatch($a)` 手动）
- v0.4.7 评估：源码层防御代价 > 收益（需要改 Laravel 框架 trait 行为或 Reflection 调用栈检测）
- v0.4.8 决定：**跳过源码层防御**，文档化已充分

---

### 4. librdkafka 1.6.2 commit 60s timeout（环境兼容，非代码 bug）

**踩坑**：`kafka:work --max-time=X` 触发强退时，librdkafka 1.6.2 + Windows + 单 broker
+ IPv4/IPv6 切换下，group commit 请求等 60s 才超时。

**现状**：长跑稳定，`max-time` 强退时才会触发，业务方非业务方代码 bug。

**修法**：
- 业务方升级 ext-rdkafka 1.9+ / 2.x（业务方编译时可能需要 librdkafka 1.9+ 系统库）
- 或者 business 端 catch `OffsetCommitRequest in flight` timeout，加 graceful close

---

## 长期 backlog（v0.4.8+ 考虑）

### 5. JsonSerializer（业务方没测，独立功能）

**踩坑**：v0.4 文档说支持 `JsonSerializer`，但 v0.4.3 hotfix 默认绑 `PhpSerializer`
（PHP serialize 格式，与 Laravel Queue 的 `Queue::push` 默认输出兼容）。
JsonSerializer 路径在测试套件里没覆盖。

**修法**：
- v0.4.8 加 unit test for `JsonSerializer::unserialize` 路径
- 加 Integration test 验证 push 出去的 payload 是 JSON 格式
- 文档里 `docs/11-Serializer.md` 已经在 v0.4 说过，业务方没真测过

---

### 6. 单元测试用 Testbench + 真 Redis（integration test 改造）

v0.4.0 unit test 默认不连 Kafka，**但 Horizon 集成测试需要真 Redis**。当前用
`Redis::connection('default')` 包装层 + 业务方本机 Redis 6379 跑通，**但 CI runner 没 Redis**。
所以 Horizon 集成测试在 CI 跑不了（run #20 tests 实际是 unit test，没有 Horizon 集成）。

**修法**（v0.4.8）：
- `.github/workflows/tests.yml` services 加 Redis service
- `tests/Integration/Horizon/` 加 e2e 测试：`KafkaQueueFake` push → `kafka:work` 消费
  → 调 `recordHorizonMetrics` → 验证 Redis 写入
- 这样 CI 也能跑 Horizon 集成

---

### 7. 业务方业务方 laravel-test 项目的清理

业务方测试项目 `laravel-test/` 在 `vendor/lyn-huang/laravel-kafka/` 是 git archive 快照
+ 手动 Copy-Item 同步 hotfix。**业务方业务环境** `composer require` 装会丢这些 hotfix。

**修法**（v0.4.8）：
- 业务方 release 后跑 `composer update lyn-huang/laravel-kafka` 重打 vendor
- 或 git clone 源码 + 手动 `cp -r src/ vendor/lyn-huang/laravel-kafka/src/`
- README 加 "Business Testing" 章节

---

## 跟踪规则

1. v0.4.7 → v0.4.8：处理 #1（phpstan 120 errors）、#2（cs-fixer 40 文件）
2. v0.4.8 → v0.5.0：处理 #3（chain 防御，已决定跳）、#4（librdkafka 升级）、#5（JsonSerializer）、#6（CI Redis service）
3. #7（laravel-test 清理）是文档性，**不**算技术债，业务方按需处理
4. 每次修完从本文件删对应条目，并在 `docs/CHANGELOG.md` 新版本里写 "Fixed" 链接回本文件
5. CI 跑 v0.4.8 时本文件应该有 0 条 high 优先级条目
