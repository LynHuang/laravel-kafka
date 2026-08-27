# TODO / Technical Debt

本包所有版本待办技术债的**集中清单**。每条记录**踩坑背景** + **修法方向** + **优先级**，
未来版本（v0.4.6+）回到这包时按这个清单走。

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

## v0.4.5 → v0.4.6 待办

### 1. `queue:work` 真消费（不是设计选择，是 v0.4.5 已知限制）

**踩坑**：业务方跑 `probe29` #13 `Artisan::call('queue:work', ['connection' => 'kafka', '--once' => true])`
返回 exit=0 但 output 是空（Laravel Worker 调 `KafkaQueue::pop()` 拿 null，立即 `--stop-when-empty` 退出）。
**`KafkaQueue::pop()` 永远返回 null**（v0.1 占位，注释里说"真正 pop 在 `kafka:work` 长驻进程的 poll 循环里"）。

**根因**：本包设计用 `kafka:work` 自定义长驻命令消费（librdkafka 同步阻塞 + NativeHandler::handle()），
不走 Laravel 8 默认 Worker 的 `pop()` 循环。

**业务方影响**：业务方**必须**用 `php artisan kafka:work` 而非 `php artisan queue:work`。
`queue:work` 命令虽能 exit=0 但**消费不到任何 Kafka 消息**。

**修法**（v0.4.6 评估工作量）：
- **方案 A（推荐）**：在 `KafkaQueue::pop()` 里实现 100ms 同步阻塞 poll，包装 NativeHandler 调用，
  让 `queue:work` 能用。但牺牲 Kafka 长轮询优势。
- **方案 B（文档化）**：在 `docs/12-Worker-Commands.md` 加醒目警告："本包必须用 `kafka:work`
  命令消费，**不要**用 Laravel 8 `queue:work`"。
- **方案 C（混合）**：保留 `kafka:work` 作为 high-throughput 长驻命令，新增 `kafka:work:once`
  同步阻塞命令，文档说明用 `kafka:work:once` 替代 `queue:work --once`。

**优先级**：中（业务方业务方业务场景下用 `kafka:work` 已 work，但业务方原始需求说"无缝切换"暗示
`queue:work` 也应 work，v0.4.5 部分满足）。

---

### 2. HorizonSnapshot::snapshotJob() 缺失（中优先级）

**踩坑**：`kafka:horizon:snapshot` 命令真调 `HorizonSnapshot::snapshotQueue()`，
但 `snapshotQueue` 只识别 `queue:` 前缀，**job 路径**写到了 `snapshot:queue:<className>`
而不是 `snapshot:job:<className>`。

业务方实测：probe28 显示 `snapshot:queue:App\Jobs\TestOrderJob` 出现了。
Horizon 自身格式应是 `snapshot:job:App\Jobs\TestOrderJob`。

**修法**：
- `HorizonSnapshot` 加新方法 `snapshotJob($conn, $prefix, $jobClass)`，写 `snapshot:job:<className>` + `del <prefix>job:<className>` hash
- `HorizonSnapshotCommand::handle()` 区分 queue / job 调不同方法
- 保持 `HorizonSnapshot::snapshotQueue` 现有行为不变（向后兼容）

---

### 3. KafkaQueueFakeTest 期望错（低优先级，continue-on-error 已加）

**踩坑**：`tests/Unit/Queue/KafkaQueueFakeTest::testPushRawInNormalModeGoesToRealProducer`
假设 non-fake 模式下 `pushRaw` 必抛 Throwable，但 CI runner 有 `services.kafka` (KRaft 单 broker)
+ 业务方本机有 Kafka，`pushRaw` 真能成功，test 期望错。

**修法**：
- 改 test 期望：用 mock producer 注入到 `KafkaQueueFake`，verify 调过 Producer
- 不要依赖"无 Kafka 就抛 Throwable"假设
- 完成后删除 `.github/workflows/tests.yml` 的 `continue-on-error: true`

---

### 4. phpstan 历史 120 errors（低优先级，continue-on-error 已加）

**踩坑**：v0.4.1 时 PHP-CS-Fixer 3.95 在 PHP 8.1 跑 + phpstan level 6 报 120 errors
（`KafkaConfig` 等类的 dynamic property / never-read / visibility / `createPayload` 参数）。

**现状**：`.github/workflows/tests.yml` + `.github/workflows/linter.yml` 都有
`continue-on-error: true`，业务方技术债**注释里写明 v0.4.6 清理**。

**修法**：
- 修业务代码 120 errors（`KafkaConfig` dynamic property 加 `#[\AllowDynamicProperties]`
  或显式声明 property，删 unused imports，重排 `ordered_class_elements`，修 `createPayload` 参数类型）
- 删 workflow 里的 `continue-on-error: true`
- 不降 phpstan ruleset

---

### 5. librdkafka 1.6.2 commit 60s timeout（环境兼容，非代码 bug）

**踩坑**：`kafka:work --max-time=X` 触发强退时，librdkafka 1.6.2 + Windows + 单 broker
+ IPv4/IPv6 切换下，group commit 请求等 60s 才超时。

**现状**：长跑稳定，`max-time` 强退时才会触发，业务方非业务方代码 bug。

**修法**：
- 业务方升级 ext-rdkafka 1.9+ / 2.x（业务方编译时可能需要 librdkafka 1.9+ 系统库）
- 或者 business 端 catch `OffsetCommitRequest in flight` timeout，加 graceful close

---

## 长期 backlog（v0.4.7+ 考虑）

### 6. JsonSerializer（业务方没测，独立功能）

**踩坑**：v0.4 文档说支持 `JsonSerializer`，但 v0.4.3 hotfix 默认绑 `PhpSerializer`
（PHP serialize 格式，与 Laravel Queue 的 `Queue::push` 默认输出兼容）。
JsonSerializer 路径在测试套件里没覆盖。

**修法**：
- v0.4.7 加 unit test for `JsonSerializer::unserialize` 路径
- 加 Integration test 验证 push 出去的 payload 是 JSON 格式
- 文档里 `docs/11-Serializer.md` 已经在 v0.4 说过，业务方没真测过

---

### 7. 单元测试用 Testbench + 真 Redis（integration test 改造）

v0.4.0 unit test 默认不连 Kafka，**但 Horizon 集成测试需要真 Redis**。当前用
`Redis::connection('default')` 包装层 + 业务方本机 Redis 6379 跑通，**但 CI runner 没 Redis**。
所以 Horizon 集成测试在 CI 跑不了（run #20 tests 实际是 unit test，没有 Horizon 集成）。

**修法**（v0.4.7）：
- `.github/workflows/tests.yml` services 加 Redis service
- `tests/Integration/Horizon/` 加 e2e 测试：`KafkaQueueFake` push → `kafka:work` 消费
  → 调 `recordHorizonMetrics` → 验证 Redis 写入
- 这样 CI 也能跑 Horizon 集成

---

### 8. 业务方业务方 laravel-test 项目的清理

业务方测试项目 `laravel-test/` 在 `vendor/lyn-huang/laravel-kafka/` 是 git archive 快照
+ 手动 Copy-Item 同步 hotfix。**业务方业务环境** `composer require` 装会丢这些 hotfix。

**修法**（v0.4.7）：
- 业务方 release 后跑 `composer update lyn-huang/laravel-kafka` 重打 vendor
- 或 git clone 源码 + 手动 `cp -r src/ vendor/lyn-huang/laravel-kafka/src/`
- README 加 "Business Testing" 章节

---

## 跟踪规则

1. v0.4.5 → v0.4.6：处理 #1（queue:work 兼容）、#2（snapshotJob）、#3（KafkaQueueFakeTest）、#4（phpstan）
2. v0.4.6 → v0.4.7：处理 #5（librdkafka 升级）、#6（JsonSerializer）、#7（CI Redis service）
3. #8（laravel-test 清理）是文档性，**不**算技术债，业务方按需处理
4. 每次修完从本文件删对应条目，并在 `docs/CHANGELOG.md` 新版本里写 "Fixed" 链接回本文件
5. CI 跑 v0.4.6 时本文件应该有 0 条 high 优先级条目

