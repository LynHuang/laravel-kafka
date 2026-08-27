# TODO / Technical Debt

本包所有版本待办技术债的**集中清单**。每条记录**踩坑背景** + **修法方向** + **优先级**，
未来版本（v0.4.5+）回到这包时按这个清单走。

来源：v0.4.0 release 后的多次业务方实测 + 后续维护中发现。

---

## v0.4.4 → v0.4.5 待办

### 1. HorizonSnapshot::snapshotJob() 缺失（中优先级）

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

### 2. KafkaQueueFakeTest 期望错（低优先级，continue-on-error 已加）

**踩坑**：`tests/Unit/Queue/KafkaQueueFakeTest::testPushRawInNormalModeGoesToRealProducer`
假设 non-fake 模式下 `pushRaw` 必抛 Throwable，但 CI runner 有 `services.kafka` (KRaft 单 broker)
+ 业务方本机有 Kafka，`pushRaw` 真能成功，test 期望错。

**修法**：
- 改 test 期望：用 mock producer 注入到 `KafkaQueueFake`，verify 调过 Producer
- 不要依赖"无 Kafka 就抛 Throwable"假设
- 完成后删除 `.github/workflows/tests.yml` 的 `continue-on-error: true`

---

### 3. phpstan 历史 120 errors（低优先级，continue-on-error 已加）

**踩坑**：v0.4.1 时 PHP-CS-Fixer 3.95 在 PHP 8.1 跑 + phpstan level 6 报 120 errors
（`KafkaConfig` 等类的 dynamic property / never-read / visibility / `createPayload` 参数）。

**现状**：`.github/workflows/tests.yml` + `.github/workflows/linter.yml` 都有
`continue-on-error: true`，业务方技术债**注释里写明 v0.4.5 清理**。

**修法**：
- 修业务代码 120 errors（`KafkaConfig` dynamic property 加 `#[\AllowDynamicProperties]`
  或显式声明 property，删 unused imports，重排 `ordered_class_elements`，修 `createPayload` 参数类型）
- 删 workflow 里的 `continue-on-error: true`
- 不降 phpstan ruleset

---

### 4. librdkafka 1.6.2 commit 60s timeout（环境兼容，非代码 bug）

**踩坑**：`kafka:work --max-time=X` 触发强退时，librdkafka 1.6.2 + Windows + 单 broker
+ IPv4/IPv6 切换下，group commit 请求等 60s 才超时。

**现状**：长跑稳定，`max-time` 强退时才会触发，业务方非业务方代码 bug。

**修法**：
- 业务方升级 ext-rdkafka 1.9+ / 2.x（业务方编译时可能需要 librdkafka 1.9+ 系统库）
- 或者 business 端 catch `OffsetCommitRequest in flight` timeout，加 graceful close
- 不阻塞 v0.4.5 release

---

### 5. JsonSerializer（业务方没测，独立功能）

**踩坑**：v0.4 文档说支持 `JsonSerializer`，但 v0.4.3 hotfix 默认绑 `PhpSerializer`
（PHP serialize 格式，与 Laravel Queue 的 `Queue::push` 默认输出兼容）。
JsonSerializer 路径在测试套件里没覆盖。

**修法**：
- v0.4.5 加 unit test for `JsonSerializer::unserialize` 路径
- 加 Integration test 验证 push 出去的 payload 是 JSON 格式
- 文档里 `docs/11-Serializer.md` 已经在 v0.4 说过，业务方没真测过

---

## 长期 backlog（v0.5+ 考虑）

### 6. 单元测试用 Testbench + 真 Redis（integration test 改造）

v0.4.0 unit test 默认不连 Kafka，**但 Horizon 集成测试需要真 Redis**。当前用
`Redis::connection('default')` 包装层 + 业务方本机 Redis 6379 跑通，**但 CI runner 没 Redis**。
所以 Horizon 集成测试在 CI 跑不了（run #20 tests 实际是 unit test，没有 Horizon 集成）。

**修法**（v0.5）：
- `.github/workflows/tests.yml` services 加 Redis service
- `tests/Integration/Horizon/` 加 e2e 测试：`KafkaQueueFake` push → `kafka:work` 消费
  → 调 `recordHorizonMetrics` → 验证 Redis 写入
- 这样 CI 也能跑 Horizon 集成

---

### 7. 业务方业务方 laravel-test 项目的清理

业务方测试项目 `laravel-test/` 在 `vendor/lyn-huang/laravel-kafka/` 是 git archive 快照
+ 手动 Copy-Item 同步 hotfix。**业务方业务环境** `composer require` 装会丢这些 hotfix。

**修法**（v0.5）：
- 业务方 release 后跑 `composer update lyn-huang/laravel-kafka` 重打 vendor
- 或 git clone 源码 + 手动 `cp -r src/ vendor/lyn-huang/laravel-kafka/src/`
- README 加 "Business Testing" 章节

---

## 跟踪规则

1. v0.4.4 → v0.4.5：处理 #1, #2, #3（#4 是环境问题可放后）
2. v0.4.5 → v0.5.0：处理 #5, #6, #7
3. 每次修完从本文件删对应条目，并在 `docs/CHANGELOG.md` 新版本里写 "Fixed" 链接回本文件
4. CI 跑 v0.4.5 时本文件应该有 0 条 high/medium 优先级条目
