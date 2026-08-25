# Changelog

本项目所有重要变更都会记录在此文件。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### v0.3.0 (planned)

详见 `RFC/0004-v0.3.md`（待写）。候选范围：
- 时间轮分层延迟消息（替代 `x-available-at` 同步阻塞）
- DLQ 高级（按 topic 分级 / 自动 replay / 限速）
- 批量消费（一次 poll 多条 → 业务方 lambda）
- 消息回放工具（replay CLI）
- 性能基准（vs mateusjunges/laravel-kafka throughput 跑分）

## [0.2.0] - 2026-08-25

### Added

#### Step 1: KafkaFake 单元测试能力

- **`FakeMessageStorage`**：记录所有 fake push 的 `['topic', 'message']` 二元组
- **`KafkaFake` 断言门面**：`assertPushed` / `assertPushedOn` / `assertPushedTimes` / `assertPushedOnTimes` / `assertNothingPushed`
- **`Kafka::fake()` Facade 静态方法**：业务方单元测试入口
- **`KafkaManager::fake() / isFake()` 标志位**：fake 模式开关
- **`KafkaQueue::pushRaw` fake 分支**：fake 模式下不真发 Kafka，写入 storage
- **4 个测试文件 / 19 个测试用例**（Storage 4 + Fake 7 + Manager 4 + Queue 4）

#### Step 2: 6 个事件系统

- **`MessagePublishing` / `MessagePublished`**：在 `KafkaQueue::pushRaw` 真发路径上 dispatch
- **`MessageConsuming` / `MessageConsumed` / `MessageFailed`**：在 `NativeHandler::handle` 业务前后 / 异常时 dispatch
- **`MessageSentToDLQ`**：在 `DlqFailedJobHandler` 写入 DLQ 后 dispatch
- **6 个事件类** + `tests/Unit/Events/EventTest.php`（7 用例）

#### Step 3: 多 Topic 路由

- **`KafkaQueue::pushRaw` 加 `options['topic']` 显式覆盖**（最优先于 `KafkaConfig::topics` 映射）
- 解析优先级：`options['topic']` → `KafkaConfig::topics` 映射 → 队列名 → `defaultTopic`
- **4 个 multi-topic 测试用例**

#### Step 4: Key Routing 顺序保证

- **`KafkaQueue::push($job, $data, $queue, ?string $key)` 第 4 参数**（向后兼容，不传 = v0.1 行为）
- 同 key 永远同 partition（librdkafka murmur2）
- **4 个 Key Routing 测试用例**

#### Step 5: Header Trace / 透传增强

- **`Header::TRACEPARENT` / `TRACESTATE` 常量**（W3C Trace Context）
- **`TraceContext` 重写**：W3C `00-<32hex>-<16hex>-<2hex>` 格式生成 / 解析 / child 派生
- **`KafkaQueue::buildMessage` 写 `traceparent` + `x-trace-id` 双 header**
- **`Consumer::wrap` 旧消息回退**：v0.1 消息（无 traceparent）自动升级
- **`options['traceparent']` 透传**：跨服务调用场景
- **10 个 Header Trace 测试用例**（TraceContext 7 + Trace 写入 3）

#### 源码 PHPDoc 中文注释

- v0.2 新增/改的 11 个 PHP 类方法 + v0.1 旧 26 个 PHP 类方法，**全部**加详细中文 PHPDoc
- 风格：类顶部 doc 写角色 / 设计动机 / 升级点 / 与 mateusjunges 差异；方法 `@param` / `@return` 中文描述 + 边界条件 + 关键点

### Fixed（v0.2 收尾时 PHP 7.4 兼容 + v0.1 老 bug）

#### PHP 7.4 兼容（composer.json 最低版本约束要求）

- **9 个文件去构造器属性提升**（`private string $x` in `__construct` → 显式 `$this->x = $x`）
  - `Config/KafkaConfig.php` / `Producer/Message.php` / `Queue/Failed/FailedContext.php` / 6 个事件类
- **5 个文件去命名参数调用**（`producev(partition: $p, ...)` → `producev($p, ...)`）
  - `Producer/Producer.php` / `Consumer/Consumer.php` / `Consumer/Handler/NativeHandler.php` / `Queue/KafkaQueue.php` / `Queue/Failed/DlqFailedJobHandler.php`
- **9 个文件去构造器参数 trailing comma**（PHP 7.4 不支持参数列表末尾的 `,`）

#### v0.1 老 bug（测试驱动发现）

- **`LaravelKafkaServiceProvider`**：`Queue::extend('kafka', ...)` 从 `register()` 移到 `boot()`（register 阶段 `queue` 容器绑定尚未建立 → Facade 解析失败）
- **`ServiceProvider::registerFailedHandlerEvent`**：`ExceptionHandler::make()` 加 `bound()` 检查（testbench 环境不一定注册）
- **`KafkaConfig::toConsumerRdKafkaConfig`**：排除 `group_id` / `auto_offset_reset` / `isolation_level` 已翻译 key（避免把业务方友好的下划线 key 透传给 librdkafka）
- **`KafkaConfig::fromArray`**：`defaultTopic` 不给默认值（缺失时让构造器抛 `KafkaException`，与 `brokers` 必填行为一致）
- **`FailedJobHandlerFactory::makeDatabase`**：`new Uuid()` 改 `Uuid::uuid4()` 静态工厂（Ramsey Uuid 4.x 构造器要 4 个必填参数）
- **`KafkaQueue`**：删 `private string $connectionName;` 重声明（父类 `Illuminate\Queue\Queue` 是 `protected`），删 `parent::__construct(null)`（父类 abstract 无显式构造器）
- **`Producer::send`**：`producev` 在 `RdKafka\ProducerTopic` 上（不在 `Producer` 上），先 `$this->kafka->newTopic($topic)` 再 `$producerTopic->producev(...)`
- **`ConnectionFactory::make`**：注入容器到 KafkaQueue（`$queue->setContainer($this->container)`）让 fake / dispatch 事件能工作
- **`KafkaQueue`**：`use LaravelKafka\Support\FakeMessageStorage` 改 `Support\Testing\FakeMessageStorage`（正确命名空间）
- **`StrTest`**：修两个 mask 测试期望字符串笔误
- **`KafkaQueueFakeTest::testPushRawInFakeModeRecordsToStorage`**：queue 参数 `'default'` 改 `null`（让 defaultTopic 兜底，与测试期望一致）

### Tests

- **86 个测试 / 186 断言全部通过**
- `tests/Unit/` 下 15 个测试文件：Config / Events / Exceptions / Manager / Producer / Queue / Support

## [0.1.0] - 2026-08-20

### Added

#### 基础功能

- `KafkaConnector`：Laravel Queue 框架识别 Kafka 驱动的入口
- `KafkaQueue`：继承 `Illuminate\Queue\Queue`，实现 push / pushRaw / later / pop / size
- `KafkaJob`：继承 `Illuminate\Queue\Jobs\Job`，包装 `RdKafka\Message`
- `kafka:work` 命令：长驻消费进程，支持 `--queue` / `--connection` / `--max-time` / `--max-jobs` / `--sleep` 参数
- SIGTERM / SIGINT 信号优雅退出

#### 失败处理（三模式可配，默认 hybrid）

- `DatabaseFailedJobHandler`：写 `failed_jobs` 表，结构与 Laravel 标准一致
- `DlqFailedJobHandler`：写 DLQ topic，注入 9 个 DLQ 专属 header
- `HybridFailedJobHandler`：双写决策树（致命异常 / 达 max_attempts 双写，未到仅写表）
- `FailedJobHandlerFactory`：按 `failed.driver` 路由到三种实现
- `FailedContext`：失败上下文值对象

#### Producer 子系统

- `Producer`：封装 `RdKafka\Producer`，同步 produce + delivery 回调
- `ProducerFactory`：构造 Conf、缓存 Producer、`flushAll` 钩子
- `Message`：不可变消息值对象（payload / headers / key / partition / timestampMs）
- `Serializer` 接口 + `PhpSerializer`（默认）/ `JsonSerializer`（v0.2 启用）

#### Consumer 子系统

- `Consumer`：封装 `RdKafka\KafkaConsumer`，poll / ack / close
- `ConsumerFactory`：构造 Conf、rebalance 回调、缓存 Consumer
- `Subscription`：订阅描述
- `HandlerInterface` / `HandlerResult`：三态结果（ack / requeue / dlq）
- `HandlerResolver` / `NativeHandler`：Laravel Job 处理入口

#### 配置与基础设施

- `KafkaConfig`：不可变配置值对象
- `KafkaManager`：多 connection 管理
- `ConnectionFactory`：KafkaQueue 装配
- `LaravelKafkaServiceProvider`：注册入口
- `config/kafka.php`：完整默认配置
- `Kafka` Facade（v0.1 仅暴露 connection / config / disconnect）

#### Support & Exceptions

- `Header`：22 个 Kafka Header 常量集中地
- `TraceContext`：trace_id 工厂
- `Str`：字节安全 truncate / mask / isUuid
- `KafkaException` / `SerializationException` / `DlqException`

#### 工具链

- `composer.json`：`lyn-huang/laravel-kafka`，PHP 7.4+ / `ext-rdkafka` 强制
- `.php-cs-fixer.php`：PER 2.0 风格
- `phpstan.neon`：level 6
- `phpunit.xml`：Unit / Feature / Integration 三个 test suite
- `.github/workflows/tests.yml`：6 组合 CI 矩阵（PHP 7.4/8.1/8.3 × Laravel 8/9/10/11）
- `.github/workflows/linter.yml`：独立 linter

#### 文档

- `开发文档_v0.1.md`：54 KB，15 章设计文档
- `docs/开发日志_v0.1.md`：50 KB，14 步脚手架实施日志（与设计文档配套）
- `README.md`：快速开始、特性矩阵、对比表
- `LICENSE`：MIT
- `RFC/0001-initial.md` / `RFC/0002-meta.md`：决策归档

### 决议

- 包名：`lyn-huang/laravel-kafka`
- PHP：7.4+（Laravel 8 ~ 11）
- ext-rdkafka：强制
- 失败模式：database / dlq / hybrid，默认 hybrid
- License：MIT
- 仓库：先私有
- Horizon / Octane / UI：v0.1 ~ v0.3 不做

### Known Issues

- Windows 上 `pcntl_signal` 不可用，按 Ctrl+C 无法触发优雅退出
- Integration 测试 CI 上 `if: false` 跳过，等 v0.2 接入 Testcontainers
- `DlqFailedJobHandler::truncate` 用 `strlen` 字节截断，对 UTF-8 多字节字符可能产生半个字
- v0.1 全部源代码未在本地实际跑过（Windows 工作区无 PHP 8.1 + rdkafka），CI 验证是首次确认

[Unreleased]: https://github.com/Lyn-Huang/laravel-kafka/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Lyn-Huang/laravel-kafka/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Lyn-Huang/laravel-kafka/releases/tag/v0.1.0
