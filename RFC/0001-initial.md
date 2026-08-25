# RFC 0001 - v0.1 初始设计

## 状态

✅ Accepted

## 日期

2026-08-20

## 摘要

确定 `laravel-kafka` v0.1 的四项基础决策：

| 项 | 决议 |
| --- | --- |
| 包名 | `lyn-huang/laravel-kafka` |
| PHP 版本下限 | 7.4+ |
| `ext-rdkafka` | 强制依赖 |
| 失败存储 | `database` / `dlq` / `hybrid` 三模式可配，默认 `hybrid` |

## 详细方案

详见 [`开发文档_v0.1.md §0、§2.2、§3.6、§6、§11.1、§13、§14`](../开发文档_v0.1.md)。

### 包名

- Composer 强制小写 → `lyn-huang/laravel-kafka`
- GitHub 仓库同 `Lyn-Huang/laravel-kafka`
- 命名空间 `LaravelKafka\`

### PHP 7.4+ 编码约束

- ❌ enum、match、readonly 属性、构造函数属性提升、命名参数、nullsafe、联合类型、独立 true/false/null 类型、first-class callable、`#[Attribute]` 注解、WeakMap/WeakReference、`str_contains` 等
- ✅ 类型声明、`void`、箭头函数、类型化属性、协变返回、数组解包、`declare(strict_types=1)`、PER 2.0 风格
- 完整对照表见 §13.1 / §13.2

### ext-rdkafka 强依赖

- `composer.json` 用 `"ext-rdkafka": "*"` 强制
- 不提供 pure-PHP fallback（事务 / consumer group rebalance 协议复杂）
- 安装说明见 README §系统要求

### 失败存储三模式

- `database`：仅写 `failed_jobs` 表（与 Laravel 默认一致）
- `dlq`：仅写 DLQ topic（不依赖 SQL）
- `hybrid`：默认。重试 < max_attempts 时仅重试；致命异常 / 达 max_attempts 时**双写** failed_jobs + DLQ
- 详细策略：§3.6.1
- 决策树由 `HybridFailedJobHandler` 实现
- DLQ 消息 9 个专属 header：§3.6.3

## 兼容性影响

- Laravel 8（PHP 7.4）~ Laravel 11（PHP 8.2+）全部矩阵覆盖
- PHP 7.4 用户被锁在 Laravel 8
- PHP 8.0/8.1 用户可用 Laravel 9/10
- PHP 8.2+ 用户可用 Laravel 11

## 替代方案

### 包名

- ❌ `your-org/laravel-kafka`：占位，无意义
- ❌ `lhk/laravel-kafka`：缩写难懂
- ✅ `lyn-huang/laravel-kafka`：识别度高，私有期唯一

### PHP 版本下限

- ❌ 8.0+：放弃 7.4 老项目
- ❌ 8.1+：与 Laravel 9 起步一致
- ✅ 7.4+：覆盖最长 LTS 时间线（7.4 = 2022-11 EOL，企业内还有大量 7.4 业务）

### ext-rdkafka 依赖

- ❌ 不强制，pure-PHP fallback：实现成本极高，事务 / rebalance 难做
- ❌ 强制 `^4.0`：与系统 librdkafka 版本耦合
- ✅ 强制 `*`：让 librdkafka 由系统包管理器装，ext-rdkafka 走 PECL 即可

### 失败模式

- ❌ 仅 database：与 Redis 队列无差异化
- ❌ 仅 dlq：用户需自建查询 UI
- ✅ 三模式 hybrid 默认：兼顾两种用户（SQL 派 / Kafka 派）

## 决策

全部 ✅ Accepted，详见 §11.1 / §14。
