# Laravel Kafka 扩展开发教程（源码精读版）

> **配套文档**：
> - 设计文档：[`开发文档_v0.1.md`](./开发文档_v0.1.md)（15 章设计，回答"应该怎么做"）
> - 实施日志：[`docs/开发日志_v0.1.md`](./docs/开发日志_v0.1.md)（14 步脚手架，回答"实际做了什么"）
> - 本教程：**源码精读**，回答"为什么这么做、这么写"
>
> **教程目标读者**：PHP 7.4 中级开发 + Laravel 应用经验 + 想深入理解一个 Composer 扩展从设计到实现的人
>
> **阅读建议**：每个 Step 配 `开发日志_v0.1.md` 的对应章节，**先看日志知道交付了什么，再看本教程理解为什么**

---

## 教程导引

### 教程结构

本教程按 14 步脚手架顺序组织。每章 = 一个 Step = 一组文件。章节内部按"文件 → 类 → 方法 → 关键行"的顺序层层拆解。

| Step | 主题 | 文件数 | 关键问题 |
| --- | --- | --- | --- |
| 1 | 仓库根文件 | 8 | Composer 怎么认这个包？Laravel 怎么发现这个扩展？ |
| 2 | 源码核心层 | 4 | 配置怎么流转？Manager 与 Factory 各管什么？ |
| 3 | Producer 子系统 | 6 | Kafka 怎么写消息？同步 vs 异步怎么选？ |
| 4 | Consumer 子系统 | 7 | Kafka 怎么读消息？Handler 怎么调度？ |
| 5 | Queue 子系统 | 3 | 怎么"伪装"成 Laravel Queue？ |
| 6 | Failed 子系统 | 6 | 失败怎么分三种模式？怎么写 DLQ？ |
| 7 | Console 命令 + 桥接 | 1 + 修补 | 长驻 worker 怎么退出？Laravel 命令怎么挂上？ |
| 8 | Support + Exceptions | 6 | 辅助类怎么设计才不会失控？ |
| 9 | Facades | 1 | Facade 是个语法糖还是真有必要？ |
| 10 | 默认配置 | 1 | 配置默认值怎么选？ |
| 11 | 测试骨架 | 11 | Testbench 怎么用？PHPUnit 9/10 怎么兼容？ |
| 12 | CI 配置 | 5 | GitHub Actions 矩阵怎么搭？ |
| 13 | RFC 归档 | 2 | 决策为什么这么定？ |
| 14 | CHANGELOG | 1 | 给用户看什么？ |

### 阅读策略

- **第一次读**：跟着章节顺序看一遍，每章代码必须**自己敲一遍**或至少**用 IDE 跳进去看完整文件**
- **第二次读**：挑你最关心的 1-2 个子系统深读（建议 Queue + Failed，laravel-kafka 的核心差异化在这）
- **第三次读**：看测试和 CI 章节，理解"怎么确保不出问题"

### 关于 PHP 7.4 兼容

整个项目受 §13 强制约束："PHP 7.4 没 enum、match、readonly、属性提升、命名参数、nullsafe"。教程里凡是出现"为什么这么写"的地方会**反复**提到这些约束怎么落地。如果你已经在 PHP 8.0+ 项目里工作，可以把本教程当作"如何在 7.4 写现代代码"的范例。

---

# Step 1 精读：仓库根文件

**目标**：让 Composer、Laravel、IDE、CI 都能识别并正确处理这个包。

8 个文件：`composer.json` / `LICENSE` / `README.md` / `.gitignore` / `.editorconfig` / `.gitattributes` / `.php-cs-fixer.php` / `phpstan.neon`

## 1.1 `composer.json` —— 包的身份证

**作用**：Composer 看这个文件来解析包的所有元信息（依赖、自动加载、脚本、包发现）。这个文件错了，整个包废了。

### 1.1.1 关键字段拆解

```json
{
    "name": "lyn-huang/laravel-kafka",
    "description": "Apache Kafka Queue driver & event bus for Laravel (PHP 7.4+).",
    "keywords": ["laravel", "kafka", "queue", "driver", "event-bus", "rdkafka"],
    "license": "MIT",
    "type": "library",
    ...
}
```

**字段设计动机**：

| 字段 | 值 | 决定动机 |
| --- | --- | --- |
| `name` | `lyn-huang/laravel-kafka` | 见 §11.1 决策。Composer 强制小写 |
| `description` | 一句话 | Packagist / IDE 索引展示用 |
| `keywords` | 6 个 | 搜索优化；不要塞 20 个，关键词越多反而不准 |
| `license` | `MIT` | 见 RFC 0002 §1 |
| `type` | `library` | 不是 plugin / metapackage / project；标准 Composer 库 |

> **不写 `type: "laravel-package"`**：很多教程会建议写这个，但其实 **没用**。Laravel 5.5+ 用 `extra.laravel.providers` 数组做包发现，写 `type: "laravel-package"` 是 Laravel 4 时代的遗留。

### 1.1.2 require 字段（运行时依赖）

```json
"require": {
    "php": ">=7.4",
    "ext-rdkafka": "*",
    "ext-json": "*",
    "illuminate/queue": "^8.0 || ^9.0 || ^10.0 || ^11.0",
    "illuminate/support": "^8.0 || ^9.0 || ^10.0 || ^11.0",
    "illuminate/console": "^8.0 || ^9.0 || ^10.0 || ^11.0",
    "illuminate/contracts": "^8.0 || ^9.0 || ^10.0 || ^11.0",
    "nesbot/carbon": "^2.0 || ^3.0",
    "ramsey/uuid": "^4.0"
},
```

**逐项解释**：

| 包 | 约束 | 为什么这么写 |
| --- | --- | --- |
| `php` | `>=7.4` | `>=` 而不是 `^7.4`，因为 `^7.4` 实际等价于 `>=7.4 <8.0`，但 `^` 在 PHP 生态里"对低版本严格、对高版本开放"是惯例 |
| `ext-rdkafka` | `*` | 强制扩展；不锁版本，让系统 librdkafka 与 PECL 装的 ext-rdkafka 自由组合 |
| `ext-json` | `*` | PHP 8.0+ 内置，但 PHP 7.4 默认装得也有，显式声明保险 |
| `illuminate/queue` | `^8.0 \|\| ^9.0 \|\| ^10.0 \|\| ^11.0` | 跨 4 个 Laravel 大版本。`^8.0` 写法等价 `>=8.0 <9.0`，`^11.0` 等价 `>=11.0 <12.0` |
| `illuminate/support` 等 | 同上 | 这 4 个 illuminate/* 必须**保持版本同步**，否则 Laravel 自身会拒绝加载 |
| `nesbot/carbon` | `^2.0 \|\| ^3.0` | Carbon 2 支持 PHP 7.4+，Carbon 3 要 PHP 8.1+；Composer 会按 PHP 版本自动选 |
| `ramsey/uuid` | `^4.0` | Laravel 自带也是 `^4.0`，跟随主版本避免双份实例 |

**为什么不写 `composer-plugin-api` / `composer-runtime-api`**：本包不是 Composer 插件，是普通库。

### 1.1.3 require-dev 字段（开发依赖）

```json
"require-dev": {
    "phpunit/phpunit": "^9.0 || ^10.0",
    "orchestra/testbench": "^6.0 || ^7.0 || ^8.0 || ^9.0",
    "mockery/mockery": "^1.0",
    "phpstan/phpstan": "^1.8",
    "friendsofphp/php-cs-fixer": "^3.0"
},
```

| 包 | 约束 | 决定动机 |
| --- | --- | --- |
| `phpunit/phpunit` | `^9 \|\| ^10` | PHPUnit 11 要 PHP 8.2+；v0.1 测试在 7.4 上必须能跑 |
| `orchestra/testbench` | `^6 \|\| ^7 \|\| ^8 \|\| ^9` | 跨 4 个 Laravel 版本。**6.x 对应 Laravel 8**，以此类推；testbench 与 Laravel 主版本严格绑定 |
| `mockery/mockery` | `^1.0` | Mock 框架；v0.1 测试用得少，但埋下伏笔 |
| `phpstan/phpstan` | `^1.8` | 静态分析；`^1.8` 包含 1.8+ 全部小版本 |
| `php-cs-fixer` | `^3.0` | 风格检查；`v3` 才有 PER Coding Style 规则 |

### 1.1.4 autoload 与 PSR-4

```json
"autoload": {
    "psr-4": {
        "LaravelKafka\\": "src/"
    }
},
"autoload-dev": {
    "psr-4": {
        "LaravelKafka\\Tests\\": "tests/"
    }
},
```

**设计动机**：

- `LaravelKafka\\` → `src/` —— 根命名空间，**所有 src 下的类都按 PSR-4 自动加载**
- `LaravelKafka\Tests\\` → `tests/` —— 测试专用，dev-only
- **为什么不写 `LaravelKafka\\Tests\\Unit\\` 多级映射**：PSR-4 是前缀匹配，写一个就够

> **不写 `classmap` / `files`**：本包没有遗留的"非 PSR-4"代码，全部 PSR-4 解决。`classmap` 会让 Composer 扫遍所有文件建索引，对本包没必要。

### 1.1.5 extra.laravel（包发现机制）

```json
"extra": {
    "laravel": {
        "providers": [
            "LaravelKafka\\LaravelKafkaServiceProvider"
        ],
        "aliases": {
            "Kafka": "LaravelKafka\\Facades\\Kafka"
        }
    }
},
```

**这是 Laravel 5.5+ 包发现机制的关键**：

- 用户装本包后，Laravel 启动时**自动**读这个 `extra.laravel.providers` 数组，把 `LaravelKafkaServiceProvider` 注册到容器
- 不需要用户在 `config/app.php` 手动加 `providers` 数组（这是 Laravel 4 时代的方式）
- `aliases` 同理：`Kafka` 全局别名自动生效

**设计取舍**：

- 严格按 Laravel 5.5+ 规范来，不向后兼容 Laravel 5.1-5.4
- 文档明确"要求 Laravel 8+"，不浪费精力兼容老版本

### 1.1.6 scripts（命令别名）

```json
"scripts": {
    "lint": "php-cs-fixer fix --dry-run --diff",
    "lint:fix": "php-cs-fixer fix",
    "static": "phpstan analyse src --level=6",
    "test": "phpunit",
    "test:unit": "phpunit --testsuite=Unit",
    "test:integration": "phpunit --testsuite=Integration"
},
```

**设计动机**：

- `composer test` 等价于 `vendor/bin/phpunit`，新人不用记具体路径
- `composer lint:fix` 修复风格，**不应**是 `composer test` 的隐式动作（避免 CI 误改）
- 三个 test 命令按 suite 拆，CI 跑 `test:unit`，本地开发可跑 `test:integration`

### 1.1.7 config（Composer 行为）

```json
"config": {
    "sort-packages": true,
    "optimize-autoloader": true
},
```

| 字段 | 作用 |
| --- | --- |
| `sort-packages` | `composer require` 时按字母序排序 require 数组；让 `composer.json` diff 干净 |
| `optimize-autoloader` | `composer install` 时生成 classmap 索引，加速自动加载 |

### 1.1.8 minimum-stability / prefer-stable

```json
"minimum-stability": "stable",
"prefer-stable": true,
```

- `minimum-stability: stable` —— 不接受 alpha / beta / RC 依赖
- `prefer-stable: true` —— 当 Carbon 3.0.0（stable）和 Carbon 3.0.0-alpha.1 同时存在时，Composer 会选 stable

**为什么需要这两个**：我们写了 `nesbot/carbon: ^2.0 || ^3.0`，但 Carbon 3 在某些时间点可能只有 alpha，必须用 `prefer-stable` 锁回 2.x。

## 1.2 `LICENSE` —— MIT 协议文本

**作用**：法律声明；用户使用、修改、分发本包的权利与义务。

**关键点**：

- 不用手写 MIT 文本——从 <https://opensource.org/licenses/MIT> 复制
- 改一行 `Copyright (c) <year> <author>`
- 这个文件**不能**删。Composer 看到 `license: MIT` 字段但仓库里没 `LICENSE` 文件，CI 工具（如 `composer-require-checker`、`composer audit`）会报警

**没有写 `LICENSE.md`**：有些项目写成 `LICENSE.md` 是错的，协议文件应该就是 `LICENSE`（无后缀）。GitHub 会自动识别并展示。

## 1.3 `README.md` —— 项目门面

**作用**：用户第一眼看到的文档。决定"这个项目值不值得花 30 秒了解"。

### 1.3.1 顶部徽章区

```markdown
[![PHP](https://img.shields.io/badge/PHP-7.4%2B-777bb4)](https://www.php.net)
[![Laravel](https://img.shields.io/badge/Laravel-8%20%7C%209%20%7C%2010%20%7C%2011-ff2d20)](https://laravel.com)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![CI](https://img.shields.io/badge/CI-7.4%20%7C%208.1%20%7C%208.3-blue)](.github/workflows/tests.yml)
```

**设计动机**：

- 4 个徽章对应"运行环境 + License + CI 状态"——一个都不能少
- shields.io 徽章格式：`<img src="...shields.io/badge/<label>-<message>-<color>">`
- 颜色用品牌色（PHP = 777bb4，Laravel = ff2d20）让徽章更专业

### 1.3.2 引用区

```markdown
> 📦 包名：`lyn-huang/laravel-kafka`
> 📚 设计文档：[`开发文档_v0.1.md`](开发文档_v0.1.md)
> 📓 实施日志：[`docs/开发日志_v0.1.md`](docs/开发日志_v0.1.md)
> 📝 变更日志：[`docs/CHANGELOG.md`](docs/CHANGELOG.md)
```

**关键**：4 个核心文档链接一目了然。emoji 让结构可视化（用户偏好 markdown 风格简洁 + 适度 emoji）。

### 1.3.3 Quick Start

```bash
composer require lyn-huang/laravel-kafka:dev-main
```

```php
'connections' => [
    'kafka' => [
        'driver'  => 'kafka',
        'name'    => 'default',
        'brokers' => env('KAFKA_BROKERS', 'localhost:9092'),
        'queue'   => env('KAFKA_DEFAULT_TOPIC', 'laravel-jobs'),
    ],
],
```

**设计动机**：

- **3 步**内让用户跑起来：`composer require` + 改 `config/queue.php` + `.env`
- 不展示完整 `config/kafka.php` 内容（太长），让用户先跑起来再去看
- 第一次接触这个包的人**最关心**："装上能不能用？" Quick Start 必须先答这个

### 1.3.4 特性矩阵

8 行 × 5 列的表格，把 v0.1 ~ v0.4 能力列出来打勾。**目的**：

- 用户一眼知道"v0.1 有什么"——大部分人会先看这一节决定是否继续
- "✅" / "—" 让功能完成度可视化
- v0.2 / v0.3 / v0.4 全是"占位"（空打勾），让用户知道路线图

### 1.3.5 对比表

8 行对比 Redis 队列与 laravel-kafka。**为什么不写 RabbitMQ 对比**：

- 我们的核心是"作为 Queue 驱动替换 Redis"，所以对比是 **Laravel Redis 队列** 而不是 RabbitMQ
- 跨生态的 MQ 对比放到了**单独的 Kafka 入门教程**里

### 1.3.6 系统要求

```markdown
- **PHP 7.4+**（8.0+ 享受 Carbon 3 与 Laravel 9+；7.4 仅 Laravel 8）
- **`ext-rdkafka`**（`pecl install rdkafka`，需要 librdkafka ≥ 1.5）
- **Laravel 8 / 9 / 10 / 11**
- **Kafka 0.11+**（KRaft 单节点或集群均可）
```

**关键**：4 个"硬依赖"必须明说，**特别**是 `ext-rdkafka`——用户最容易在这里卡住。

### 1.3.7 FAQ

7 个常见问题：

1. 顺序保证怎么配？—— v0.2 才有 Key Routing
2. 失败任务怎么排查？—— `queue:failed` 命令 + DLQ topic
3. 能重放历史吗？—— v0.2 才有
4. 多业务方独立消费？—— v0.3 才有
5. Kafka 集群怎么起？—— 链到 Kafka 入门教程
6. ext-rdkafka 怎么装？—— 链到 §1.3.6
7. Windows 能用吗？—— 优雅降级，pcntl 不可用

**为什么 FAQ 这么详细**：v0.1 是新项目，没有 Stack Overflow 答案兜底，README FAQ 是用户的第一道防线。

## 1.4 `.gitignore` —— 排除不该入库的文件

**作用**：防止 vendor/、IDE 缓存、coverage 报告等污染 git 历史。

```gitignore
/vendor/
/composer.lock
/.phpunit.result.cache
/.phpunit.cache/
/build/
/coverage/
/.idea/
/.vscode/
*.swp
*.swo
*~
.DS_Store
Thumbs.db
desktop.ini
.env
.env.local
.env.*.local
*.log
/storage/logs/*.log
/.php-cs-fixer.cache
/.phpstan.cache
*.html
```

**关键决策**：

- **`composer.lock` 不入库**：本包是**库**（library），不是应用（application）。lock 文件意义在于"团队成员用相同依赖版本"——但本包的"用户"是外部项目，每个项目有自己的 lock
- **`.env` 不入库**：保护用户隐私
- **`/storage/logs/*.log`**：虽然是 Library 用不到，但 Laravel 项目会拷我们的结构，先防一手
- **`/coverage/` 和 `*.html`**：防止 PHPUnit 生成的 HTML 报告污染

> **为什么不忽略 `phpunit.xml`**：这个文件**应该**入库——它定义了 PHPUnit 行为，新人 clone 完直接能跑测试。

## 1.5 `.editorconfig` —— 跨编辑器一致

**作用**：让 VSCode、PHPStorm、Sublime 在不同人手里格式一样。

```ini
root = true

[*]
charset = utf-8
end_of_line = lf
indent_size = 4
indent_style = space
insert_final_newline = true
trim_trailing_whitespace = true

[*.md]
trim_trailing_whitespace = false

[*.{yml,yaml}]
indent_size = 2

[composer.json]
indent_size = 4
```

**设计动机**：

- **root = true**：仓库根是规范源，子目录不再找上级
- **`trim_trailing_whitespace = true`**：默认 trim；md 文件特批**不**trim（中文行末两个空格 = 强制换行，trim 掉就破坏 markdown）
- **yml 缩进 2 空格**：YAML 习惯（虽然 PHP-CS-Fixer 不管 YAML，但 `.github/workflows/*.yml` 需要）
- **`composer.json` 4 空格**：JSON 习惯

**为什么不写 `max_line_length`**：PHP-CS-Fixer 已经在 config 里管 120 软宽了，不重复。

## 1.6 `.gitattributes` —— Git 行为控制

**作用**：

1. 控制 `git archive` 打 zip / tarball 时排除哪些文件
2. 控制 `git diff` 对特定文件的处理
3. 文件 EOL 统一（防止 Windows commit 引入 CRLF）

```gitattributes
/.github                export-ignore
/.gitattributes         export-ignore
/.gitignore             export-ignore
/.editorconfig          export-ignore
/.php-cs-fixer.php      export-ignore
/phpstan.neon           export-ignore
/phpunit.xml            export-ignore
/docs                   export-ignore
/RFC                    export-ignore
/tests                  export-ignore
/.phpunit.result.cache  export-ignore
/.phpunit.cache         export-ignore
/.php-cs-fixer.cache    export-ignore
/.phpstan.cache         export-ignore
/coverage               export-ignore
/composer.lock          export-ignore
*.md                    text eol=lf
*.php                   text eol=lf
*.json                  text eol=lf
*.yml                   text eol=lf
*.yaml                  text eol=lf
```

**`export-ignore` 设计动机**：

当用户下载本包的 `tarball`（Packagist 提供的 dist）时，**不应该**包含：

- `.github/`（CI 配置只对开发有意义）
- `docs/` / `RFC/`（文档放在仓库，dist 不带）
- `tests/`（测试代码对用户运行没意义）
- `phpunit.xml` / `.php-cs-fixer.php` / `phpstan.neon`（开发工具配置）
- 各种 `.cache` 目录

**`text eol=lf` 设计动机**：

强制所有文本文件是 LF 行尾。Windows 用户 commit 时 Git 会自动转换 CRLF → LF，Mac/Linux 用户不变。最终仓库里所有 .md / .php / .json / .yml 全部是 LF。

## 1.7 `.php-cs-fixer.php` —— 代码风格规则

**作用**：自动修复 PHP 代码风格（缩进、引号、import 排序等），让团队代码风格统一。

```php
<?php

declare(strict_types=1);

$finder = PhpCsFixer\Finder::create()
    ->in(__DIR__ . '/src')
    ->in(__DIR__ . '/tests')
    ->name('*.php')
    ->ignoreDotFiles(true)
    ->ignoreVCS(true);
```

**逐行解释**：

- `declare(strict_types=1);` —— 所有 PHP 文件**必须**严格类型，这是项目铁律
- `Finder::create()->in(...)` —— 扫描 `src/` 与 `tests/` 目录
- `->name('*.php')` —— 只匹配 .php 文件
- `->ignoreDotFiles(true)` —— 不扫描 `.git/` 之类的隐藏目录
- `->ignoreVCS(true)` —— 不扫描 `.git/`

```php
return (new PhpCsFixer\Config())
    ->setRiskyAllowed(true)
    ->setRules([
        '@PER' => true,
        '@PER:risky' => true,
        ...
    ])
    ->setFinder($finder)
    ->setCacheFile(__DIR__ . '/.php-cs-fixer.cache');
```

**关键设计**：

- `setRiskyAllowed(true)` —— 启用"有风险"规则集（如 `declare_strict_types` 强制）。必须显式开启
- `@PER` + `@PER:risky` —— 加载 PER Coding Style 2.0（PSR-12 升级版）。**比 `@PSR12` 更现代、更符合 Laravel 生态**
- `setCacheFile(...)` —— 缓存修复结果，大仓库下能提速 10x

### 1.7.1 关键规则逐条解释

```php
'@PER' => true,
'@PER:risky' => true,
'@PHP71Migration' => true,
'@PHP74Migration' => true,
```

- `@PER` —— PER 2.0 全部规则
- `@PER:risky` —— 风险规则（如把 `==` 改 `===`、删 `@` 抑制符）
- `@PHP71Migration` / `@PHP74Migration` —— 保证代码在 PHP 7.4 也能跑（不用 8.0+ 语法）

```php
'ordered_imports' => [
    'sort_algorithm' => 'alpha',
    'imports_order' => ['class', 'function', 'const'],
],
```

- **为什么按字母序排 import**：让 `git diff` 干净（加一个新 use，不会引发整块重排）
- `imports_order`：`class` 在前（最常见），然后 `function`（如 `use function ...`），最后 `const`（如 `use const ...`）

```php
'php_unit_method_casing' => ['case' => 'camel_case'],
```

- 测试方法名必须驼峰（`testXxx`），不能用 snake_case

```php
'php_unit_test_case_static_method_calls' => ['call_type' => 'self'],
```

- 父类静态方法调用用 `self::` 而不是 `static::`。Laravel 风格的代码里 `self::` 更常见

```php
'no_unused_imports' => true,
```

- 不允许 `use X;` 之后不用 X。死代码会被清理

```php
'single_quote' => true,
```

- 字符串用单引号（除非有插值）。**PHP 性能习惯**，单引号无变量解析

```php
'trailing_comma_in_multiline' => [
    'elements' => ['arrays', 'arguments', 'parameters'],
],
```

- 多行数组 / 函数参数最后一个元素后加逗号。**为什么**：以后加新元素时 `git diff` 干净（只加一行，不动上一行）

```php
'declare_strict_types' => true,
```

- 强制每个文件首行 `declare(strict_types=1);`（如果缺失则加）

```php
'array_syntax' => ['syntax' => 'short'],
```

- 数组用 `[]` 而不是 `array()`。**PHP 5.4+ 通用**，符合现代风格

```php
'no_useless_else' => true,
'no_useless_return' => true,
```

- 删掉没用的 `else` 和 `return` —— 让代码逻辑更直白

```php
'ordered_class_elements' => [
    'order' => [
        'constant_public',
        'constant_protected',
        'constant_private',
        'property_public_static',
        'property_protected_static',
        'property_private_static',
        'property_public',
        'property_protected',
        'property_private',
        'method_public_static',
        'method_protected_static',
        'method_private_static',
        'construct',
        'destruct',
        'magic',
        'phpunit',
        'method_public',
        'method_protected',
        'method_private',
    ],
],
```

**类成员排序规则**：

1. 常量：public → protected → private
2. 静态属性：public → protected → private
3. 实例属性：public → protected → private
4. 静态方法：public → protected → private
5. 构造 / 析构 / 魔术方法
6. PHPUnit 方法（如果有）
7. 实例方法：public → protected → private

**为什么这样排**：越公开的越靠前，越常用的越靠前。IDE 跳转时也符合阅读顺序。

## 1.8 `phpstan.neon` —— 静态分析

**作用**：在不运行代码的情况下，发现类型错误、未使用变量、错误的函数调用等。

```neon
parameters:
    level: 6
    paths:
        - src
    excludePaths:
        - src/Exceptions
    ignoreErrors:
        - '#Call to an undefined method RdKafka\\.*#'
    treatPhpDocTypesAsCertain: false
    checkMissingIterableValueType: false
    reportUnmatchedIgnoredErrors: false
```

**逐项解释**：

- `level: 6` —— PHPStan 严格度 0-9，6 是 v0.1 目标（不上 8/9 是因为 7.4 兼容灵活度）
  - level 0：基本语法
  - level 3：所有类型
  - level 6：魔法方法、类型推断
  - level 9：所有可能的最大严格度
- `paths: src` —— 只扫源码，不扫 tests（tests 用 PHPUnit 而不是 PHPStan 验证）
- `excludePaths: src/Exceptions` —— 异常类通常空字段，让 PHPStan 跳过
- `ignoreErrors: '#Call to an undefined method RdKafka\\\\.*#'` —— ext-rdkafka 反射由 librdkafka 动态生成，PHPStan 静态推断会报错，统一忽略
- `treatPhpDocTypesAsCertain: false` —— `@param Foo $x` 不强制 `Foo` 真的存在（避免 mock 时报错）
- `checkMissingIterableValueType: false` —— 不强制 `array` 标成 `array<Foo>`
- `reportUnmatchedIgnoredErrors: false` —— 已经从 ignoreErrors 移除的规则不报警

**为什么不 level 8**：

- level 8 要求所有方法都写返回类型（包括 void）
- level 8 要求严格处理 `mixed` 类型
- 对一个 7.4 兼容包来说太严格，留出灵活度

## 1.9 Step 1 小结

| 文件 | 行数 | 价值 | 出错代价 |
| --- | --- | --- | --- |
| `composer.json` | ~50 | 决定 Composer / Laravel 怎么用本包 | **致命**——错了完全装不上 |
| `LICENSE` | ~20 | 法律合规 | **致命**——企业用户无法采用 |
| `README.md` | ~150 | 用户第一印象 | 高——影响采用率 |
| `.gitignore` | ~30 | 仓库清洁 | 中——污染历史 |
| `.editorconfig` | ~20 | 跨编辑器一致 | 低 |
| `.gitattributes` | ~30 | dist 包清洁 / EOL 统一 | 中——发布时炸 |
| `.php-cs-fixer.php` | ~80 | 风格统一 | 中——风格不统一 |
| `phpstan.neon` | ~15 | 静态检查 | 低——质量下降 |

**关键认知**：

1. **`composer.json` 决定一切**：name 错、version 错、require 错——任何一个失误整个包废了
2. **`extra.laravel` 是 5.5+ 包发现机制**：不要写 `type: "laravel-package"`，那是 Laravel 4 时代
3. **`.gitattributes` 的 `export-ignore`**：用户从 Packagist 下载的 dist 不应包含测试和 CI 文件
4. **`@PER` 比 `@PSR12` 强**：现代化的 PER 2.0 才是 PHP 生态的趋势

---

# Step 2 精读：源码核心层

**目标**：实现"配置 → Manager → Connection → Queue"的装配链。

4 个文件：`src/Config/KafkaConfig.php` / `src/Manager/KafkaManager.php` / `src/Manager/ConnectionFactory.php` / `src/LaravelKafkaServiceProvider.php`

## 2.1 `src/Config/KafkaConfig.php` —— 不可变配置值对象

**作用**：把 `config/kafka.connections.default` 数组翻译成强类型对象，让所有 Kafka 客户端调用都通过这个对象拿配置。

### 2.1.1 整体结构

```php
final class KafkaConfig
{
    public function __construct(
        private string $name,
        private string $brokers,
        private string $clientId,
        private string $protocol,
        private array $sasl,
        private array $ssl,
        private string $defaultTopic,
        private array $topics,
        private array $producer,
        private array $consumer,
        private array $failed,
        private array $delay,
        private array $replay,
    ) {
        $this->validateProtocol($protocol);
        if ($brokers === '') {
            throw new KafkaException('Kafka brokers must not be empty.');
        }
        if ($defaultTopic === '') {
            throw new KafkaException('Kafka default topic must not be empty.');
        }
    }
    ...
}
```

**关键设计**：

1. **`final class`**：禁止继承。配置值对象不需要扩展点，继承反而会让"配置来自哪里"变模糊
2. **构造函数属性提升 (`private string $name`)**：因为 PHP 8.0 才支持，**我们用 PHP 7.4 不能写 `public function __construct(public string $name)`**——只能显式 `private` 声明 + 赋值
3. **所有字段 `private`**：外部不能改，只能通过命名访问器（`name()` / `brokers()` 等）读
4. **构造时校验**：3 个不变量在构造时验证（protocol 必须 ∈ 4 种之一、brokers 非空、defaultTopic 非空）

**为什么不写 getter (`getName()`)**：

PHP 8 之前的命名习惯是 `getName()`，但现代 PHP 推荐**只写名字**（`$config->name()`）—— 类似 Java 的 `record`、Kotlin 的 `val`。这样代码读起来更像"读一个字段"。

### 2.1.2 `resolveTopic(?string $queue): string` —— 业务方法

```php
public function resolveTopic(?string $queue): string
{
    if ($queue !== null && isset($this->topics[$queue])) {
        return (string) $this->topics[$queue];
    }
    if ($queue !== null && $queue !== '') {
        return $queue;
    }
    return $this->defaultTopic;
}
```

**设计动机**：

3 段式 fallback 解决"队列名 → topic 名"映射：

1. **显式映射**：`$this->topics['emails'] = 'app.emails'` 时，逻辑队列 `emails` → 物理 topic `app.emails`
2. **同名**：用户没配映射，把队列名当 topic 名
3. **默认**：用户没传 queue（`null` 或空），用 `defaultTopic`

**关键认知**：方法**不是** `resolveTopic(string $queue)` 而是 `resolveTopic(?string $queue)` —— 接受 null 是因为 Laravel 的 `push($job, $data, null)` 允许 queue 为空。

### 2.1.3 `toProducerRdKafkaConfig()` / `toConsumerRdKafkaConfig()` —— 翻译层

```php
public function toProducerRdKafkaConfig(): array
{
    return array_merge($this->toRdKafkaConfig(), $this->stringifyConfig($this->producer));
}
```

**设计动机**：

Kafka 客户端（librdkafka）配置是字符串键值对：

```php
['bootstrap.servers' => 'localhost:9092', 'client.id' => 'app', 'compression.codec' => 'snappy']
```

但我们的内部配置是结构化 PHP 数组：

```php
['brokers' => 'localhost:9092', 'client_id' => 'app', 'producer' => ['compression' => 'snappy']]
```

`toProducerRdKafkaConfig` 把内部结构翻译成 librdkafka 期望的扁平结构。**为什么分两方法**：Producer 与 Consumer 配置有差异（`acks` vs `group.id`），不能共用。

### 2.1.4 `toRdKafkaConfig()` —— 通用部分

```php
public function toRdKafkaConfig(): array
{
    $conf = [
        'client.id' => $this->clientId,
        'bootstrap.servers' => $this->brokers,
    ];

    if ($this->protocol === 'SSL' || $this->protocol === 'SASL_SSL') {
        foreach ($this->ssl as $k => $v) {
            if ($v !== null && $v !== '') {
                $conf['ssl.' . $this->sslKeyMap((string) $k)] = (string) $v;
            }
        }
    }

    if ($this->protocol === 'SASL_PLAINTEXT' || $this->protocol === 'SASL_SSL') {
        ...
        $conf['security.protocol'] = $this->protocol;
    } elseif ($this->protocol === 'SSL') {
        $conf['security.protocol'] = 'SSL';
    }

    return $conf;
}
```

**关键设计**：

- **SSL/SASL 字段按 protocol 注入**：只当 `protocol === 'SSL'` 才注入 `ssl.*` 字段；只当 SASL 才注入 `sasl.*` 字段
- **为什么？** librdkafka 对未知 protocol / 字段会报 `Invalid config value`。宁可不注也不要错注
- **`sslKeyMap` 短名 → 长名映射**：用户配置 `ca_location`，librdkafka 要 `ssl.ca.location`——名字风格不一致是 librdkafka 自己的问题，我们做一层翻译

### 2.1.5 私有 `stringifyConfig()`

```php
private function stringifyConfig(array $src): array
{
    $out = [];
    foreach ($src as $k => $v) {
        if ($v === null) {
            continue;
        }
        if (is_bool($v)) {
            $out[(string) $k] = $v ? 'true' : 'false';
        } elseif (is_array($v) || is_object($v)) {
            continue;
        } else {
            $out[(string) $k] = (string) $v;
        }
    }
    return $out;
}
```

**逐分支解释**：

- `null` → 跳过（不写进 librdkafka config）
- `bool` → 转字符串 `'true'` / `'false'`（librdkafka 期望字符串）
- `array` / `object` → 跳过（librdkafka 不接受嵌套结构）
- 其他 → 转字符串（int / float / string 都接受）

**为什么不直接 `(string) $v`**：因为 PHP 序列化 bool 时 `true → "1"`，`false → ""`——与 librdkafka 期望的 `'true'` / `'false'` 不一致。需要显式分支。

### 2.1.6 静态工厂 `fromArray()`

```php
public static function fromArray(string $name, array $config): self
{
    return new self(
        name: $name,
        brokers: (string) ($config['brokers'] ?? ''),
        clientId: (string) ($config['client_id'] ?? 'laravel-kafka'),
        protocol: (string) ($config['protocol'] ?? 'PLAINTEXT'),
        sasl: (array) ($config['sasl'] ?? []),
        ssl: (array) ($config['ssl'] ?? []),
        defaultTopic: (string) ($config['queue'] ?? 'laravel-jobs'),
        ...
    );
}
```

**注意**：这是 **PHP 8.0+ 的命名参数**语法。**我们 PHP 7.4 不能用**！

**实际项目里怎么写**：

```php
return new self(
    $name,                                         // name
    (string) ($config['brokers'] ?? ''),           // brokers
    (string) ($config['client_id'] ?? 'laravel-kafka'),  // clientId
    (string) ($config['protocol'] ?? 'PLAINTEXT'), // protocol
    (array) ($config['sasl'] ?? []),               // sasl
    (array) ($config['ssl'] ?? []),                // ssl
    (string) ($config['queue'] ?? 'laravel-jobs'),  // defaultTopic
    (array) ($config['topics'] ?? []),             // topics
    (array) ($config['producer'] ?? []),           // producer
    (array) ($config['consumer'] ?? []),           // consumer
    (array) ($config['failed'] ?? []),             // failed
    (array) ($config['delay'] ?? []),              // delay
    (array) ($config['replay'] ?? []),             // replay
);
```

> **为什么写命名参数伪代码**：教程为了让读者看清"哪个数组 key 对应哪个构造参数"，**实际项目里 7.4 只能用位置参数**。这是 PHP 7.4 兼容项目的一个真实痛点。

**`??` 空合并运算符**：PHP 7.0+ 支持，**不是 7.4 新增**。但本项目大量使用，比 `isset($x) ? $x : 'default'` 简洁。

### 2.1.7 私有 `validateProtocol()` / `sslKeyMap()`

```php
private function validateProtocol(string $protocol): void
{
    $allowed = ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'];
    if (! in_array($protocol, $allowed, true)) {
        throw new KafkaException(sprintf(
            'Invalid Kafka protocol "%s". Allowed: %s',
            $protocol,
            implode(', ', $allowed)
        ));
    }
}
```

**关键**：

- **`in_array(..., $allowed, true)`** —— 第三参数 `true` 表示**严格比较**。`in_array` 默认是松散比较（`0 == "PLAINTEXT"` 也是 true），这会让 `0` 这种异常输入"通过校验"。7.4 没 `enum`，只能用字符串白名单 + 严格比较

```php
private function sslKeyMap(string $short): string
{
    $map = [
        'ca_location' => 'ca.location',
        'cert_location' => 'cert.location',
        'key_location' => 'key.location',
        'key_password' => 'key.password',
    ];
    return $map[$short] ?? $short;
}
```

**设计动机**：用户友好配置（`ca_location`）→ librdkafka 内部命名（`ca.location`）的翻译。`?? $short` 兜底：未知短名原样透传（librdkafka 仍可能识别）。

## 2.2 `src/Manager/KafkaManager.php` —— 多 connection 管理

**作用**：让用户能配置多个 Kafka connection（`default` / `reports` / `emails`），通过名字拿到对应 `Queue` 实例。

### 2.2.1 整体结构

```php
final class KafkaManager
{
    private array $connections = [];
    private array $configs = [];
    private ConnectionFactory $factory;

    public function __construct(ConnectionFactory $factory)
    {
        $this->factory = $factory;
    }

    public function connection(?string $name = null): Queue
    {
        $name = $name ?? $this->getDefaultConnection();
        if (! isset($this->connections[$name])) {
            $config = $this->resolveConfig($name);
            $this->connections[$name] = $this->factory->make($config, $name);
        }
        return $this->connections[$name];
    }
    ...
}
```

**关键设计**：

1. **两个缓存数组**：`connections` 存 `Queue` 实例，`configs` 存 `KafkaConfig` 实例。**为什么不合一**：因为 `KafkaConfig` 可能被 KafkaQueue 之外的组件用（如 `KafkaManager::config()` 直接暴露给用户），分开缓存更清晰
2. **懒加载**：`connection()` 第一次被调才 `factory->make()`，避免启动时把所有 Kafka 集群都连上
3. **`name` 解析顺序**：用户传的 `$name` → 默认为 `config('kafka.default')` → 最终兜底 `'default'`

### 2.2.2 `getDefaultConnection()` 的兜底

```php
private function getDefaultConnection(): string
{
    $name = (string) (function_exists('config') ? config('kafka.default', 'default') : 'default');
    if ($name === '') {
        $name = 'default';
    }
    return $name;
}
```

**为什么 `function_exists('config')` 兜底**：

- 正常情况：Laravel 容器启动时已经 `register()` 把 ServiceProvider 走完，`config()` 全局函数可用
- 单元测试场景：Testbench 启动的容器 mock 有时 `config()` 不存在
- 这是一个**逃生口**，正常调用一定走 Laravel 容器

### 2.2.3 `disconnect()` 留给 Octane

```php
public function disconnect(?string $name = null): void
{
    $name = $name ?? $this->getDefaultConnection();
    unset($this->connections[$name], $this->configs[$name]);
}
```

**设计动机**：

- 普通 Laravel 进程（请求结束 → 进程结束）不需要 disconnect
- **Laravel Octane** 是常驻进程模型（`swoole` / `roadrunner`），需要主动释放 Kafka 连接避免 fd 泄漏
- v0.1 不做 Octane 适配（§11.1 决议），但**接口先留好**

## 2.3 `src/Manager/ConnectionFactory.php` —— 装配入口

**作用**：根据 `KafkaConfig` 构造 `KafkaQueue`，把 Producer / Consumer / FailedHandler 三个子系统的入口拼起来。

```php
final class ConnectionFactory
{
    private ProducerFactory $producerFactory;
    private ConsumerFactory $consumerFactory;
    private FailedJobHandlerFactory $failedHandlerFactory;

    public function __construct(
        ProducerFactory $producerFactory,
        ConsumerFactory $consumerFactory,
        FailedJobHandlerFactory $failedHandlerFactory
    ) {
        $this->producerFactory = $producerFactory;
        $this->consumerFactory = $consumerFactory;
        $this->failedHandlerFactory = $failedHandlerFactory;
    }

    public function make(KafkaConfig $config, string $connectionName): KafkaQueue
    {
        $producer = $this->producerFactory->make($config);
        $consumer = $this->consumerFactory->make($config);
        $failedHandler = $this->failedHandlerFactory->make($config);

        return new KafkaQueue(
            $producer,
            $consumer,
            $failedHandler,
            $config,
            $connectionName,
        );
    }
}
```

**关键设计**：

1. **三个子 Factory 由容器注入**：不自己 new，遵守 Laravel DI 约定
2. **`make()` 是单方法**：`KafkaConfig → KafkaQueue` 的纯函数（除缓存外）
3. **为什么不缓存 `KafkaQueue` 实例**：因为 Manager 已经缓存了，Factory 不重复缓存

**为什么不让 Manager 直接 new**：

- Manager 负责"管理连接"
- Factory 负责"造一个连接"
- 测试时可以把 MockFactory 注入 Manager 测 Manager 行为
- 这种"职责分离"是组合模式的标准做法

## 2.4 `src/LaravelKafkaServiceProvider.php` —— 注册入口

**作用**：让 Laravel 知道本扩展存在、配置怎么注入、命令怎么注册、失败处理怎么挂上。

### 2.4.1 `register()` vs `boot()` 的时机

```php
public function register(): void
{
    $this->mergeConfigFrom(...);
    $this->app->singleton(ConnectionFactory::class, ...);
    $this->app->singleton(KafkaManager::class, ...);
    $this->app->alias(KafkaManager::class, 'kafka.manager');
    Queue::extend('kafka', function ($app) { return new KafkaConnector(...); });
}

public function boot(): void
{
    $this->syncFailedTableConfig();
    $this->registerFailer();
    $this->registerFailedHandlerEvent();
    $this->registerCommands();
    $this->registerPublishing();
}
```

**Laravel 启动顺序铁律**：

| 阶段 | 干什么 | 能/不能 |
| --- | --- | --- |
| `register()` | 绑定类到容器、合并 config | **不能**用 `app('config')->get('xxx')`（配置还没全部 load） |
| `boot()` | 读 config、注册事件、注册命令、发布资源 | **能**用 `config('kafka.*')` |

**为什么 `Queue::extend('kafka', ...)` 在 `register()`**：

- `Queue::extend` 接受一个 `Connector` 工厂闭包
- 当 `config/queue.php` 里 `driver => 'kafka'` 被解析时，Laravel 容器**会**回头调 `queue.connector.kafka` 容器键
- 容器键的解析可能在 `boot()` 之前发生（取决于应用复杂程度）
- 所以 `Queue::extend` 必须**早**于实际 `Queue` 实例化

### 2.4.2 三个 singleton 绑定

```php
$this->app->singleton(ConnectionFactory::class, function ($app) {
    return new ConnectionFactory(
        $app->make(\LaravelKafka\Producer\ProducerFactory::class),
        $app->make(\LaravelKafka\Consumer\ConsumerFactory::class),
        $app->make(FailedJobHandlerFactory::class),
    );
});

$this->app->singleton(KafkaManager::class, function ($app) {
    $manager = new KafkaManager($app->make(ConnectionFactory::class));
    $connections = (array) config('kafka.connections', []);
    $manager->registerConnections($connections);
    return $manager;
});

$this->app->alias(KafkaManager::class, 'kafka.manager');
```

**`singleton` 含义**：

- 第一次 `app(KafkaManager::class)` 时**实例化 + 缓存**
- 之后 `app(KafkaManager::class)` / `app('kafka.manager')` 都拿**同一个实例**
- **为什么用 singleton**：KafkaManager 持有 Producer / Consumer / Config 缓存，重复创建会破坏缓存语义

**`registerConnections()` 在 register 阶段调用**：

- `config('kafka.connections')` 在 `register()` 里**可以**读——因为本 ServiceProvider 先 `mergeConfigFrom`，所以 `kafka.connections` 一定有默认值
- 但 `config('queue.connections.kafka')` 在 `register()` 里**可能**是空——这是另一个 connection 名空间，不归本包管

### 2.4.3 `registerFailer()` —— failed_jobs 表的容器绑定

```php
private function registerFailer(): void
{
    $driver = (string) config('kafka.connections.default.failed.driver', 'hybrid');
    if ($driver === 'dlq') {
        return;
    }

    $this->app->singleton('queue.failer.kafka', function ($app) {
        $config = (array) config('kafka.connections.default.failed.database', []);
        $table = (string) ($config['table'] ?? 'failed_jobs');
        $connection = $config['connection'] ?? null;

        $database = $app->make('db')->connection($connection);

        return new DatabaseFailedJobHandler(
            $database,
            $table,
            $app->make(\Ramsey\Uuid\UuidInterface::class)
        );
    });
}
```

**`queue.failer.kafka` 这个键的意义**：

- Laravel 内部 `Illuminate\Queue\Failed\FailedJobProviderInterface` 的实现要求容器里有名为 `queue.failer` 的绑定
- 但 Laravel 的 `config/queue.php` 只支持**单一** failer driver
- 本包**没**替换 Laravel 内部的 `queue.failer`，而是另起一个 `queue.failer.kafka` 容器键
- `KafkaJob::fail()` 自己查这个键

**为什么 dlq 模式跳过**：

- dlq 模式不写 `failed_jobs` 表，没必要注册 failer
- 让 `php artisan queue:failed` 命令在 dlq 模式下行为是"no failed jobs"（Laravel 默认）

### 2.4.4 `syncFailedTableConfig()` —— Laravel 命令桥接

```php
private function syncFailedTableConfig(): void
{
    $driver = (string) config('kafka.connections.default.failed.driver', 'hybrid');
    if ($driver === 'dlq') {
        return;
    }
    $table = (string) config('kafka.connections.default.failed.database.table', 'failed_jobs');
    config(['queue.failed' => array_merge(
        (array) config('queue.failed', []),
        ['driver' => 'database-uuids', 'database' => config('kafka.connections.default.failed.database.connection'), 'table' => $table]
    )]);
}
```

**为什么需要这一步**：

- 用户配置 `kafka.connections.default.failed.database.table = 'my_failed_jobs'`
- 但 Laravel 的 `php artisan queue:failed` 命令读 `config('queue.failed.table')`
- 两者不一致时，Laravel 命令会读不到我们的表
- 解决办法：启动时把 `kafka.*.table` 同步到 `queue.failed.*`，让 Laravel 命令与我们的 handler 共享同一表

**`driver: 'database-uuids'`**：

- Laravel 8+ 的 `database-uuids` driver 对应 `DatabaseUuidFailedJobProvider`
- 这个 Provider 默认读 `config('queue.failed.table')`——和我们的 `failed_jobs` 表兼容
- 我们的 `DatabaseFailedJobHandler` 独立实现了 `FailedJobHandlerInterface`，但**底层表结构一致**

### 2.4.5 `registerFailedHandlerEvent()` —— 事件占位

```php
private function registerFailedHandlerEvent(): void
{
    $events = $this->app->make('events');
    $events->listen(JobFailed::class, function (JobFailed $event) {
        try {
            $queue = $event->connectionName === 'kafka'
                ? $this->app->make('kafka.manager')->connection()
                : null;
            if ($queue === null) {
                return;
            }
            $job = $event->job;
            // KafkaJob::fail() 已经自己处理了 failed handler 的分发
            // 此处仅占位，留给 v0.2 完善
            unset($job);
        } catch (\Throwable $e) {
            $handler = $this->app->make(ExceptionHandler::class);
            $handler->report($e);
        }
    });
}
```

**设计动机**：

- v0.1 失败处理由 `KafkaJob::fail()` 主动调用 FailedJobHandler
- 但 Laravel 的 `Illuminate\Queue\Worker` 在业务失败时也会**触发** `JobFailed` 事件
- 本 listener 占位，v0.2+ 用于接告警 / 监控 / metrics
- `try/catch` 包住全部逻辑，任何异常通过 `ExceptionHandler::report` 报出去，但**不**让 ServiceProvider 启动失败

**为什么 `unset($job)`**：

- PHP 7.4 lint 工具（如 phpstan）会警告"unused variable $job"
- v0.1 不使用，标记为"将来用"
- v0.2 实现告警时删掉 `unset($job)`

### 2.4.6 `registerCommands()` / `registerPublishing()`

```php
private function registerCommands(): void
{
    if ($this->app->runningInConsole()) {
        $this->commands([
            WorkCommand::class,
        ]);
    }
}

private function registerPublishing(): void
{
    if ($this->app->runningInConsole()) {
        $this->publishes([
            __DIR__ . '/../config/kafka.php' => config_path('kafka.php'),
        ], 'kafka-config');
    }
}
```

**`runningInConsole()`**：

- Web 请求（HTTP 进程）里**不**注册命令、不发布资源
- 只有 CLI 进程才注册——避免 Web 进程浪费内存
- 这是 Laravel 通用最佳实践

**`commands([...])`**：把 `WorkCommand` 注册成 `php artisan kafka:work` 命令

**`publishes([...], 'kafka-config')`**：让用户跑 `php artisan vendor:publish --tag=kafka-config` 时把 `config/kafka.php` 复制到 `config/kafka.php`（应用层）。**用户应该用这个命令而不是手工复制**。

## 2.5 Step 2 小结

| 类 | 职责 | 关键设计 |
| --- | --- | --- |
| `KafkaConfig` | 配置值对象，翻译 librdkafka 配置 | 不可变 + 构造时校验 + resolveTopic 业务方法 |
| `KafkaManager` | 多 connection 管理 + 懒加载缓存 | 区分 connections/configs 两个缓存 |
| `ConnectionFactory` | 装配 Producer/Consumer/FailedHandler | 单 make 方法，DI 三个子 Factory |
| `LaravelKafkaServiceProvider` | 注册入口 | register 阶段绑定 + boot 阶段读 config + Laravel 5.5+ 包发现 |

**关键认知**：

1. **`KafkaConfig` 是单一真相源**：所有 Kafka 客户端都从这一个对象读配置。`toProducerRdKafkaConfig()` / `toConsumerRdKafkaConfig()` 让翻译层集中可测
2. **`KafkaManager` 与 `ConnectionFactory` 职责分离**：Manager 管"用哪个 connection"，Factory 管"怎么造 connection"
3. **ServiceProvider 启动顺序铁律**：`Queue::extend` 必须在 `register()` 而不是 `boot()`，否则容器解析可能找不到 connector
4. **`syncFailedTableConfig()` 的存在意义**：让 Laravel 自己的 `queue:failed` 命令能与本包共用表。**没有这一步，用户的 `queue:failed` 永远是空的**

---

# Step 3 精读：Producer 子系统

**目标**：把"业务消息"可靠地发到 Kafka。

6 个文件：`Message` / `Serializer` 接口 + 2 实现 / `Producer` / `ProducerFactory`

## 3.1 `src/Producer/Message.php` —— 不可变消息值对象

**作用**：把"消息"封装成强类型对象。Producer / Consumer 两侧都过这个对象。

### 3.1.1 字段设计

```php
final class Message
{
    public function __construct(
        private string $payload,
        private array $headers = [],
        private ?string $key = null,
        private ?int $partition = null,
        private ?int $timestampMs = null,
    ) {
    }
}
```

**5 个字段**：

| 字段 | 类型 | 含义 | 必填 |
| --- | --- | --- | --- |
| `payload` | `string` | 消息体（**已序列化**） | ✅ |
| `headers` | `array<string,string>` | Kafka header | ❌ |
| `key` | `?string` | 路由键，决定 partition | ❌ |
| `partition` | `?int` | 显式指定 partition（生产侧不推荐） | ❌ |
| `timestampMs` | `?int` | 消息时间戳（ms），null = broker 当前时间 | ❌ |

**为什么是 `string payload` 而不是 `mixed`**：

- `Message` 是"已序列化"层——序列化由 `Serializer` 接口负责
- 值对象层**不**做序列化，保持职责单一
- `Producer::send` 收 `Message`，拿到的 payload 已经是字符串，直接给 librdkafka

**为什么 key 是 `?string` 不是 `string`**：

- 大部分消息不需要 key
- `null` 让 librdkafka 走"轮询分区"（每条消息随机落 partition）
- 显式 `null` 比显式空字符串好——空字符串会被 librdkafka 算成 key（murmur2('') → partition 0）

### 3.1.2 链式 `withXxx` 方法

```php
public function withHeaders(array $headers): self
{
    return new self(
        payload: $this->payload,
        headers: array_merge($this->headers, $headers),
        key: $this->key,
        partition: $this->partition,
        timestampMs: $this->timestampMs,
    );
}
```

**设计动机**：

- `Message` 是不可变（immutable）的——`withHeaders` 返回**新实例**
- 不在原对象上 mutate，避免上游代码意外修改
- 链式调用：`(new Message('a'))->withHeader('x-trace', 'abc')->withKey('user-1')`

**为什么不支持 fluent chain 到 send**：

- Producer.send 不是 Message 的方法（职责分离）
- 调用方显式 `producer->send($topic, $message)`，避免隐式副作用

### 3.1.3 `header(string $name, ?string $default = null)`

```php
public function header(string $name, ?string $default = null): ?string
{
    return $this->headers[$name] ?? $default;
}
```

**设计动机**：

- 取单个 header 的便利方法
- 不存在时返回 null（让 `??` 链自然工作）
- 也支持 `header('x-trace', 'unknown')` 直接给默认值

## 3.2 `src/Producer/Serializer/Serializer.php` —— 序列化器接口

**作用**：把"业务对象"翻译成"字节流"。Consumer 侧用同样的 `Serializer::decode()` 还原。

```php
interface Serializer
{
    public function encode(mixed $value): string;
    public function decode(string $raw);
    public function name(): string;
}
```

**3 个方法的设计动机**：

| 方法 | 为什么 | 替代方案 |
| --- | --- | --- |
| `encode(mixed $value): string` | 入参任意类型（数组 / 对象 / 标量） | 拆成 `encodeArray` / `encodeObject` 不灵活 |
| `decode(string $raw): mixed` | 返回任意类型 | 返回 array 太严格（不允许对象） |
| `name(): string` | 写进 Header，消费端按 name 选 decoder | 文档化约定易出错 |

**`name()` 的关键作用**：

- Producer 写消息时注入 `x-serializer: php`（或 `json`）header
- Consumer 读消息时按 `x-serializer` 选对应 decoder
- **这就是消息自描述**——同一份消息流可以混用多种序列化器

**为什么不固定 `php` 序列化**：

- v0.1 默认 `PhpSerializer`（与 Laravel 兼容）
- v0.2 启用 `JsonSerializer`（跨语言消费）
- 用户可自定义 `AvroSerializer`（v0.4 配合 Schema Registry）

## 3.3 `src/Producer/Serializer/PhpSerializer.php` —— 与 Laravel 兼容

**作用**：v0.1 默认序列化器，与 Laravel `Illuminate\Queue\Queue::createPayload` 行为一致。

```php
public function encode($value): string
{
    try {
        return serialize($value);
    } catch (\Throwable $e) {
        throw new SerializationException(
            sprintf('PhpSerializer encode failed: %s', $e->getMessage()),
            0,
            $e
        );
    }
}
```

**关键设计**：

- **包 try/catch**：`serialize()` 通常不会失败，但当 value 包含 closure / resource 等不可序列化对象时会触发 `SerializationException`
- **包成 `SerializationException`**：业务层 catch 一种异常，不让 PHP 内置 `\Throwable` 逃逸

```php
public function decode(string $raw)
{
    if ($raw === '') {
        return null;
    }
    try {
        $value = unserialize($raw, ['allowed_classes' => true]);
    } catch (\Throwable $e) {
        throw new SerializationException(
            sprintf('PhpSerializer decode failed: %s', $e->getMessage()),
            0,
            $e
        );
    }
    if ($value === false && $raw !== 'b:0;') {
        throw new SerializationException('PhpSerializer decode returned false on non-empty payload.');
    }
    return $value;
}
```

**两个关键点**：

1. **空字符串返回 `null`**：消费端用空 payload 表示"心跳"或"哨兵"，不抛异常
2. **`$raw !== 'b:0;'` 区分**：PHP `serialize(false)` 结果是 `'b:0;'`——这是合法的"序列化 false"。但 `unserialize` 失败时也会返回 `false`——这是异常。要区分这两种情况

**`unserialize($raw, ['allowed_classes' => true])`**：

- 第二个参数 `allowed_classes` 控制反序列化时允许哪些类
- `true` = 允许所有类
- `false` = 全部转 `__PHP_Incomplete_Class`（不安全数据用）
- `['Foo', 'Bar']` = 只允许这两个类

**为什么用 `true`**：与 Laravel 默认行为一致。Laravel 的 `Illuminate\Bus\PendingDispatch` 反序列化时也是 `allowed_classes=true`。

## 3.4 `src/Producer/Serializer/JsonSerializer.php` —— 跨语言友好

**作用**：v0.2 启用的 JSON 序列化器，让 Node / Go / Python 消费者能直接读 Laravel Job。

```php
public function __construct()
{
    $this->depth = 512;
    $this->flags = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES;
}

public function encode($value): string
{
    try {
        $json = json_encode($value, $this->flags, $this->depth);
    } catch (\Throwable $e) {
        throw new SerializationException(...);
    }
    if ($json === false) {
        throw new SerializationException(sprintf('JsonSerializer encode failed: %s', json_last_error_msg()));
    }
    return $json;
}
```

**关键设计**：

- `JSON_UNESCAPED_UNICODE` —— 中文不被转成 `\uXXXX`（保留可读性）
- `JSON_UNESCAPED_SLASHES` —— URL 里的 `/` 不被转成 `\/`（保留 URL 形态）
- `depth = 512` —— PHP 7.4 默认值
- **`json_encode` 返回 false 时抛异常**：json_encode 失败返回 false 而不是抛异常，必须显式判断

```php
public function decode(string $raw)
{
    if ($raw === '') {
        return null;
    }
    try {
        $value = json_decode($raw, true, $this->depth, $this->flags);
    } catch (\Throwable $e) {
        throw new SerializationException(...);
    }
    if ($value === null && $raw !== 'null') {
        throw new SerializationException(sprintf('JsonSerializer decode failed: %s', json_last_error_msg()));
    }
    return $value;
}
```

**关键点**：

- `json_decode` 第二参数 `true` —— 返回**关联数组**而不是对象（避免 PHP-side 类型处理复杂）
- `null` 区分：JSON 字符串 `'null'` 解码后是 `null`（合法），但其他非空字符串解码失败也是 `null`（异常）。要区分

## 3.5 `src/Producer/Producer.php` —— 同步等待的 Producer

**作用**：封装 `RdKafka\Producer`，把"业务发消息"翻译成"librdkafka produce 调用 + 同步等待 delivery 报告"。

### 3.5.1 构造函数

```php
public function __construct(RdKafkaProducer $kafka)
{
    $this->kafka = $kafka;
}
```

**`RdKafka\Producer` 是 librdkafka PHP 绑定的 C 类**。PHP 侧只是持有它，不做额外初始化。

### 3.5.2 同步等待的 send

```php
public function send(string $topic, Message $message): int
{
    $partition = $message->partition() ?? RD_KAFKA_PARTITION_UA;
    $key = $message->key();
    $headers = $this->normalizeHeaders($message->headers());

    $this->lastDeliverySucceeded = false;
    $token = random_int(0, PHP_INT_MAX);

    $this->deliveryCallbacks[$token] = [
        'topic' => $topic,
        'key' => $key,
    ];

    $this->kafka->producev(
        partition: $partition,
        msgflags: 0,
        payload: $message->payload(),
        key: (string) $key,
        headers: $headers,
        timestamp_ms: $message->timestampMs() ?? 0,
        opaque: (string) $token,
    );

    $start = microtime(true);
    $timeoutMs = 5000;
    while (! $this->lastDeliverySucceeded && (microtime(true) - $start) * 1000 < $timeoutMs) {
        $this->kafka->poll(50);
    }

    unset($this->deliveryCallbacks[$token]);

    if (! $this->lastDeliverySucceeded) {
        throw new KafkaException(...);
    }

    return $partition;
}
```

**逐行解析**：

1. `$partition = $message->partition() ?? RD_KAFKA_PARTITION_UA`
   - `RD_KAFKA_PARTITION_UA` 是 librdkafka 常量 `-1`（"unassigned"，由 broker 选 partition）
   - 显式用 `??` 兜底 `null` → UA

2. `random_int(0, PHP_INT_MAX)` —— 用密码学安全随机数当 token
   - 为什么用 token：`producev` 的最后一个参数 `opaque` 用来在 delivery 回调里**找回**这条消息
   - 一个 Producer 实例可能并发 produce 多条（虽然 v0.1 是同步），token 是它们的 ID
   - `random_int` 而不是 `rand`——确保唯一性，避开 PRNG 周期问题

3. `$this->kafka->producev(...)` —— 真正的 Kafka produce
   - `producev` 是 `produce` 的扩展版，支持 headers / timestamp / opaque
   - 注意：这里还是用**命名参数**伪代码展示。**PHP 7.4 必须用位置参数**：

   ```php
   $this->kafka->producev(
       $partition,                              // partition
       0,                                        // msgflags
       $message->payload(),                     // payload
       (string) $key,                            // key
       $headers,                                 // headers
       $message->timestampMs() ?? 0,             // timestamp_ms
       (string) $token                           // opaque
   );
   ```

4. **同步轮询循环** —— 5s 内等 delivery 报告
   - `$this->kafka->poll(50)` —— poll 50ms 让 librdkafka 处理 delivery 回调
   - 5s 超时——超出抛 `KafkaException`
   - **为什么同步等**：业务层期望"发完即落地"，避免"以为发出去实际没到"

5. `unset($this->deliveryCallbacks[$token])` —— 清理回调引用，防止内存泄漏

### 3.5.3 delivery 回调

```php
public function handleDeliveryReport(\RdKafka\Message $msg): void
{
    $this->lastDeliverySucceeded = ($msg->err === RD_KAFKA_RESP_ERR_NO_ERROR);
    if (! $this->lastDeliverySucceeded) {
        $token = (int) $msg->opaque;
        $context = $this->deliveryCallbacks[$token] ?? ['topic' => '?', 'key' => null];
        error_log(sprintf(
            '[laravel-kafka] produce failed: topic=%s key=%s err=%d %s',
            $context['topic'],
            $context['key'] ?? '(null)',
            $msg->err,
            rd_kafka_err2str($msg->err)
        ));
    }
}
```

**关键设计**：

- **回调只置标志位**：`$this->lastDeliverySucceeded` 是简单的 bool
- **`send()` 同步轮询时检查标志位**——避免在回调里抛异常（回调抛异常会让 librdkafka 行为不可预测）
- **失败时 `error_log`**：记录 topic / key / 错误码，但**不**抛异常
- **`$msg->opaque`**：从 librdkafka 找回 token，再找 `deliveryCallbacks` 数组里的上下文

**为什么不接 Laravel Log**：

- v0.1 简化：直接 `error_log` 到 PHP 错误日志
- v0.2 评估接入 `Psr\Log\LoggerInterface`——通过 ServiceProvider 注入

### 3.5.4 `flush()`

```php
public function flush(int $timeoutMs = 10000): void
{
    $code = $this->kafka->flush($timeoutMs);
    if ($code !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new KafkaException(sprintf('Kafka flush failed with code %d.', $code));
    }
}
```

**为什么 Worker 退出前必须 flush**：

- librdkafka 默认是**异步**发送——`producev` 调用只是把消息塞到本地 buffer
- 真正发到 broker 由后台线程做
- Worker 进程如果直接退出，buffer 里的消息就丢了
- `flush(timeoutMs)` 阻塞等所有 in-flight 消息投递完成

### 3.5.5 `fromConf()` 静态工厂

```php
public static function fromConf(Conf $conf): self
{
    $instance = new self(new RdKafkaProducer($conf));
    $conf->setDrMsgCb(function ($kafka, $message) use ($instance) {
        $instance->handleDeliveryReport($message);
    });
    return $instance;
}
```

**为什么需要静态工厂**：

- `RdKafka\Conf` 必须在 `new RdKafka\Producer` **之前**配好
- 但 `setDrMsgCb` 需要 `$instance` 引用（回调闭包 use）
- 顺序：new instance → 绑 setDrMsgCb（用 instance 引用）
- 但 Conf 必须在 new RdKafka\Producer 之前传给构造器
- 解决方法：**先 new 一个临时 producer，再绑回调**

**为什么不直接接受 Conf 而非 RdKafka\Producer**：

- 测试时希望 mock 一个 producer（不真连 Kafka）
- 接受 `RdKafka\Producer` 让测试可以传 mock 子类

## 3.6 `src/Producer/ProducerFactory.php` —— 工厂与缓存

**作用**：单连接内单例缓存 Producer，提供 `flushAll` 钩子给 Worker 退出前清理。

```php
final class ProducerFactory
{
    private array $instances = [];

    public function make(KafkaConfig $config): Producer
    {
        $key = $config->name();
        if (! isset($this->instances[$key])) {
            $this->instances[$key] = $this->build($config);
        }
        return $this->instances[$key];
    }

    public function flushAll(int $timeoutMs = 10000): void
    {
        foreach ($this->instances as $producer) {
            try {
                $producer->flush($timeoutMs);
            } catch (\Throwable $e) {
                error_log('[laravel-kafka] flush failed: ' . $e->getMessage());
            }
        }
    }

    private function build(KafkaConfig $config): Producer
    {
        $conf = new Conf();
        $rdConfig = $config->toProducerRdKafkaConfig();
        foreach ($rdConfig as $k => $v) {
            $conf->set((string) $k, (string) $v);
        }
        $conf->setErrorCb(function ($kafka, $err, $reason) {
            error_log(sprintf(
                '[laravel-kafka] producer error: code=%d reason=%s',
                $err,
                (string) $reason
            ));
        });
        $conf->setLogCb(function ($kafka, $level, $facility, $message) {
            if ((int) $level <= 3) {
                error_log(sprintf(
                    '[laravel-kafka] librdkafka log: level=%d facility=%s message=%s',
                    $level,
                    (string) $facility,
                    (string) $message
                ));
            }
        });

        return Producer::fromConf($conf);
    }
}
```

**关键设计**：

### 3.6.1 `instances` 单例缓存

- `instances[$config->name()]` —— 同一 connection 只 new 一次 Producer
- **为什么缓存**：Producer 内部有 librdkafka 后台线程 + 内存 buffer；new 多次会重复开线程，浪费资源

### 3.6.2 `flushAll()` 钩子

- **为什么不自动 flush**：
  - Producer 不知道自己什么时候"用完"
  - 业务方（Worker / WorkCommand）决定退出时机
  - 工厂提供 `flushAll()` 让外部调

### 3.6.3 `setErrorCb` / `setLogCb` —— 只 log 不抛

- `setErrorCb` 接到 librdkafka 错误（连接失败 / 鉴权失败 / etc）—— 记录到 PHP 错误日志
- `setLogCb` 接到 librdkafka 内部日志（debug 级），**只** level ≤ 3 记录（error / warning / notice），避免 debug 噪音
- **为什么不抛异常**：这两个回调可能在 librdkafka 后台线程触发，从后台线程抛异常会让 PHP 崩溃

## 3.7 Step 3 小结

| 类 | 职责 | 关键设计 |
| --- | --- | --- |
| `Message` | 不可变消息值对象 | 5 字段 + 链式 withXxx + header 访问器 |
| `Serializer` 接口 | 序列化器契约 | encode / decode / name |
| `PhpSerializer` | v0.1 默认 | 与 Laravel 兼容 + `b:0;` 边界 |
| `JsonSerializer` | v0.2 启用 | UNESCAPED_UNICODE/SLASHES + null 区分 |
| `Producer` | librdkafka produce 封装 | 同步等 delivery + token 标识 + flush 兜底 |
| `ProducerFactory` | 单例缓存 + 错误回调 | 跨 connection 缓存 + flushAll 钩子 |

**关键认知**：

1. **`Message` 不可变 + 链式 with**：让上游代码无法意外修改已构造消息
2. **同步等 delivery**：业务层期望"发完即落地"，但 librdkafka 实际是异步——5s 同步等是务实妥协
3. **`name()` 让序列化器自描述**：同一 topic 可以混用 php / json，Consumer 按 header 选 decoder
4. **回调里只 log 不抛异常**：librdkafka 回调可能在后台线程触发，抛异常会让 PHP 崩溃

---

# Step 4 精读：Consumer 子系统

**目标**：从 Kafka 拉消息、路由到 handler、提交 offset。

7 个文件：`Subscription` / `HandlerInterface` / `HandlerResult` / `HandlerResolver` / `NativeHandler` / `Consumer` / `ConsumerFactory`

## 4.1 `src/Consumer/Subscription.php` —— 订阅描述

**作用**：把"Worker 订阅哪些 topic"封装成值对象。

```php
final class Subscription
{
    private array $topics;

    public function __construct(array $topics)
    {
        if (count($topics) === 0) {
            throw new \InvalidArgumentException('Subscription requires at least one topic.');
        }
        $this->topics = array_values(array_unique(array_map('strval', $topics)));
    }

    public function topics(): array
    {
        return $this->topics;
    }

    public function firstTopic(): string
    {
        return $this->topics[0];
    }
}
```

**关键设计**：

- **`count($topics) === 0` 校验**：订阅空 topic 列表没意义，构造时拒掉
- **`array_values(array_unique(array_map('strval', $topics)))`**：三步去重 + 强转 string
  - `strval` —— 防御性，防止传 int（如 `0`）当 topic
  - `array_unique` —— 同一 topic 写多次去重
  - `array_values` —— 重新索引为 0, 1, 2, ...（保证 `topics[0]` 可用）

**v0.1 简化**：只支持 topic 列表。v0.3 扩展 per-topic handler / 自定义选项。

## 4.2 `src/Consumer/Handler/HandlerInterface.php` —— 处理器契约

```php
interface HandlerInterface
{
    public function handle(Message $message): HandlerResult;
}
```

**为什么是单方法接口**：

- Handler 模式的标准：输入消息，返回结果
- 让实现类只关心"怎么消费消息"，不暴露"从哪里订阅""提交 offset"等细节
- 测试时 mock 一个 Handler 极简

**为什么不返回 `void`**：

- 消费完必须告诉 Worker："这条消息后续怎么办"（ack / requeue / dlq）
- `HandlerResult` 是这个决策的封装

## 4.3 `src/Consumer/Handler/HandlerResult.php` —— 三态结果

**作用**：把 Handler 的处理结果封装成三种动作之一。

```php
final class HandlerResult
{
    public const ACTION_ACK = 'ack';
    public const ACTION_REQUEUE = 'requeue';
    public const ACTION_DLQ = 'dlq';

    private string $action;
    private ?\Throwable $error;

    public function __construct(string $action, ?\Throwable $error = null)
    {
        if (! in_array($action, [self::ACTION_ACK, self::ACTION_REQUEUE, self::ACTION_DLQ], true)) {
            throw new \InvalidArgumentException(sprintf('Invalid handler action: %s', $action));
        }
        $this->action = $action;
        $this->error = $error;
    }

    public static function ack(): self
    {
        return new self(self::ACTION_ACK);
    }

    public static function requeue(\Throwable $error = null): self
    {
        return new self(self::ACTION_REQUEUE, $error);
    }

    public static function dlq(\Throwable $error): self
    {
        return new self(self::ACTION_DLQ, $error);
    }
    ...
}
```

**关键设计**：

### 4.3.1 三个动作的语义

| 动作 | 含义 | 何时使用 |
| --- | --- | --- |
| `ack` | 提交 offset，消息从主 topic 视角移除 | 业务成功 |
| `requeue` | 重新入队到主 topic，retry_count++ | 业务失败但还有重试机会 |
| `dlq` | 写入 DLQ topic，提交 offset | 业务失败且到 max_attempts，或致命异常 |

### 4.3.2 用类常量模拟 enum

- PHP 7.4 没有 enum（8.1+ 才有）
- `public const ACTION_* = '...'` 模拟 enum
- `in_array(..., $allowed, true)` 严格比较校验

### 4.3.3 静态工厂方法

- `HandlerResult::ack()` —— 不带 error
- `HandlerResult::requeue($error)` —— error 可选
- `HandlerResult::dlq($error)` —— error 必填

**为什么 requeue 的 error 是 optional**：

- v0.2+ 可能支持"主动 requeue"（不是失败，而是排到下一档 delay queue），此时没有异常
- v0.1 实现里 requeue 总是有 error，但接口预留扩展

## 4.4 `src/Consumer/Handler/HandlerResolver.php` —— handler 路由

**作用**：根据消息元数据决定交给哪个 handler 处理。

```php
final class HandlerResolver
{
    private NativeHandler $nativeHandler;

    public function __construct(NativeHandler $nativeHandler)
    {
        $this->nativeHandler = $nativeHandler;
    }

    public function resolve(string $topic, Message $message): HandlerInterface
    {
        // v0.1：所有消息都按 Laravel Job 处理
        // v0.3：根据 $message->header('x-handler') 路由到不同 handler
        return $this->nativeHandler;
    }
}
```

**v0.1 单路 / v0.3 多路**：

- v0.1：所有消息都返回 `NativeHandler`（Laravel Job 处理）
- v0.3：按 `x-handler` header 路由——比如 `x-handler: native` 走 NativeHandler，`x-handler: webhook` 走 WebhookHandler

**为什么不在 v0.1 就实现多路**：

- 简单优先——v0.1 把"作为 Laravel 队列驱动"这条主路径走通
- v0.3 多 handler 是"扩展功能"，与"基础能力"分开迭代
- 接口已留好，v0.3 改 `resolve()` 方法体即可

## 4.5 `src/Consumer/Handler/NativeHandler.php` —— Laravel Job 桥

**作用**：v0.1 唯一的 Handler，把 Kafka 消息翻译成 Laravel Job，调 `Worker::process` 执行。

### 4.5.1 整体流程

```php
public function handle(Message $message): HandlerResult
{
    $queue = $this->container->make('kafka.manager')->connection();
    if (! $queue instanceof KafkaQueue) {
        throw new KafkaException('Default queue is not a KafkaQueue instance.');
    }

    $job = $this->createJob($message, $queue);

    $maxAttempts = (int) ($this->container->make('config')->get(
        'kafka.connections.default.failed.hybrid.max_attempts',
        3
    ));

    $options = new WorkerOptions(
        name: 'kafka-default',
        backoff: '0',
        memory: 128,
        timeout: 60,
        sleep: 1,
        maxTries: $maxAttempts,
        force: false,
        stopWhenEmpty: false,
        maxJobs: 0,
        maxTime: 0,
        rest: 0,
    );

    try {
        $this->worker->process(
            $this->container->make('config')->get('queue.default', 'kafka'),
            $job,
            $options
        );
        return HandlerResult::ack();
    } catch (Throwable $e) {
        return $this->onException($job, $message, $e);
    }
}
```

**逐段解释**：

#### 第一段：拿 KafkaQueue 实例

- `$this->container->make('kafka.manager')->connection()` —— 从容器拿 Manager，再拿默认 connection
- **为什么不直接注入 KafkaQueue**：因为不同 connection 的 KafkaQueue 不一样（default / reports / emails），handler 应该是通用的

#### 第二段：构造 WorkerOptions

- `new WorkerOptions(...)` 是 Laravel 8+ 的 API
- 字段：
  - `name` —— worker 名称，metrics 用
  - `backoff` —— 失败重试间隔（秒），'0' = 无 backoff
  - `memory` —— 内存上限（MB），超过自动重启
  - `timeout` —— 单个 Job 超时（秒）
  - `sleep` —— 无消息时 sleep 秒数
  - `maxTries` —— 最大重试次数，从 config 读
  - `force` —— 强制跑过期 Job
  - `stopWhenEmpty` —— 队列空时退出（kafka:work 不设这个，因为 Kafka 永远可能来新消息）
  - `maxJobs` / `maxTime` —— 限次 / 限时

#### 第三段：调 Worker::process

- `$this->worker->process($connectionName, $job, $options)` —— Laravel 内部标准方法
- `$connectionName` 从 `config('queue.default')` 拿——保证 Worker 知道当前是哪个 connection
- **成功 → `HandlerResult::ack()`**
- **失败 → `onException()` 决定 ack / requeue / dlq**

### 4.5.2 `createJob()` 构造伪 RdKafka\Message

```php
private function createJob(Message $message, KafkaQueue $queue): KafkaJob
{
    $consumer = $this->container->make(\LaravelKafka\Consumer\Consumer::class);

    $raw = $message->payload();
    $headers = $message->headers();

    $rdMsg = new \RdKafka\Message();
    $rdMsg->payload = $raw;
    $rdMsg->headers = $headers;
    $rdMsg->key = $message->key();
    $rdMsg->partition = $message->partition() ?? 0;
    $rdMsg->offset = (int) ($headers['x-offset'] ?? 0);
    $rdMsg->topic_name = $message->header('x-original-topic') ?? 'laravel-jobs';

    $queueName = (string) ($headers['x-queue'] ?? 'default');
    $connectionName = (string) ($headers['x-connection'] ?? 'kafka');

    return new KafkaJob(
        $this->container,
        $consumer,
        $rdMsg,
        $connectionName,
        $queueName,
    );
}
```

**为什么构造伪 `RdKafka\Message`**：

- `KafkaJob`（Step 5 实现）接受 `RdKafka\Message` 是为了拿原始 partition / offset / topic_name
- 我们手里只有 `LaravelKafka\Producer\Message`（Producer 子系统的值对象）
- 需要把 `Producer\Message` 翻译成 `RdKafka\Message` 给 `KafkaJob`

**为什么用公共属性赋值而不是构造器**：

- `RdKafka\Message` 没有 setter，全是公共属性
- `new \RdKafka\Message()` 后直接赋值
- **已知风险**（来自开发日志）：librdkafka 某些版本 `headers` 是只读属性

### 4.5.3 `onException()` 失败处理

```php
private function onException(KafkaJob $job, Message $message, Throwable $e): HandlerResult
{
    $driver = (string) $this->container->make('config')->get(
        'kafka.connections.default.failed.driver',
        'hybrid'
    );

    if ($driver === 'database') {
        return HandlerResult::ack();
    }

    $this->failedHandler->handle(
        $job,
        $e,
        new \LaravelKafka\Queue\Failed\FailedContext(
            $message->payload(),
            (array) $message->headers(),
            $message->header('x-original-topic') ?? 'laravel-jobs',
            $message->partition() ?? 0,
            (int) ($message->header('x-attempt') ?? 0),
        )
    );

    $isFatal = $this->isFatalException($e);
    $attempt = (int) ($message->header('x-attempt') ?? 0) + 1;
    $maxAttempts = (int) $this->container->make('config')->get(
        'kafka.connections.default.failed.hybrid.max_attempts',
        3
    );

    if ($isFatal || $attempt >= $maxAttempts) {
        return HandlerResult::dlq($e);
    }

    return HandlerResult::requeue($e);
}
```

**逐段解析**：

1. **database 模式**：直接 ack（让 Laravel 自己的 `failed_jobs` 流程接管）
2. **dlq / hybrid 模式**：先调 `failedHandler->handle()` 写失败
3. **决策树**：
   - 致命异常 OR 重试超限 → DLQ
   - 其他 → requeue（重试）

**关键**：`failedHandler->handle()` 的实现已经在 Step 6 详细讲了（Database / Dlq / Hybrid 三种）。

### 4.5.4 `isFatalException()`

```php
private function isFatalException(Throwable $e): bool
{
    $fatal = (array) $this->container->make('config')->get(
        'kafka.connections.default.failed.hybrid.fatal_exceptions',
        []
    );
    foreach ($fatal as $class) {
        if ($e instanceof $class) {
            return true;
        }
    }
    return false;
}
```

**致命异常列表**：

- `SerializationException` —— 序列化失败，重试无意义
- `ValidationException` —— 输入不合法，重试不会变
- `ClassNotFoundException` —— 业务类不存在，重试不会变

**为什么 `instanceof`**：用 `instanceof` 比 `get_class($e) === $fatal` 灵活——支持子类匹配（如 `MyValidationException extends ValidationException`）。

## 4.6 `src/Consumer/Consumer.php` —— 拉消息与提交 offset

**作用**：封装 `RdKafka\KafkaConsumer`，提供 `poll` / `ack` / `close`。

### 4.6.1 构造与订阅

```php
public function __construct(RdKafkaConsumer $kafka, Subscription $subscription)
{
    $this->kafka = $kafka;
    $this->subscription = $subscription;

    $this->kafka->subscribe($subscription->topics());
}
```

**`subscribe()` 在构造时调**：

- 让 Consumer 实例从诞生起就订阅 topic
- 之后不需要再次 `subscribe()`
- 重新订阅需要 new 一个新的 Consumer

**为什么不暴露 `subscribe` / `unsubscribe` 方法**：

- v0.1 简单：Consumer = 一组 topic + 一组 group_id
- 动态改订阅是 v0.3 才有的需求

### 4.6.2 `poll()`

```php
public function poll(int $timeoutMs = 1000): ?Message
{
    $rdMsg = $this->kafka->consume($timeoutMs);

    switch ($rdMsg->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            return $this->wrap($rdMsg);

        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            return null;

        default:
            throw new KafkaException(sprintf(
                'Kafka consume error: code=%d %s',
                $rdMsg->err,
                rd_kafka_err2str($rdMsg->err)
            ));
    }
}
```

**三态分支**：

- `NO_ERROR` —— 有消息，包装成 `Message` 返回
- `PARTITION_EOF` / `TIMED_OUT` —— 没消息（EOF 是 partition 读完，TIMED_OUT 是 poll 超时），返回 `null` 让 Worker 继续循环
- 其他错误 —— 抛 `KafkaException`

**为什么不区分 EOF 与 TIMED_OUT**：

- Worker 视角都是"没拉到消息"，继续循环即可
- 真要区分，调用方拿到 `null` 后可以做额外处理（如记录"最近 N 秒内无消息"告警）

### 4.6.3 `ack()` 与 `commitAsync()`

```php
public function ack(RdKafkaMessage $rdMessage): void
{
    $this->kafka->commit($rdMessage);
}

public function commitAsync(): void
{
    $this->kafka->commitAsync();
}
```

- `commit($msg)` —— 同步 commit 单条消息的 offset
- `commitAsync()` —— 异步 commit 所有 in-flight offset

**为什么用 `commit` 而不 `commitAsync`**：

- 同步 commit 保证消息业务处理完 + offset 持久化 才返回
- 异步 commit 性能更好但有"消息处理成功但 commit 失败"的小概率窗口
- v0.1 选择同步——简单优先

### 4.6.4 `close()`

```php
public function close(): void
{
    try {
        $this->kafka->close();
    } catch (\Throwable $e) {
        error_log('[laravel-kafka] consumer close error: ' . $e->getMessage());
    }
}
```

**为什么 close 兜底 try/catch**：

- librdkafka close 可能因为 rebalance 进行中、网络异常等情况抛错
- Worker 退出时 close 失败不应该让进程崩溃（已经到退出阶段）
- 静默 log 即可

### 4.6.5 `wrap()` 注入 Kafka 侧 header

```php
private function wrap(RdKafkaMessage $rdMsg): Message
{
    $headers = [];
    if (is_array($rdMsg->headers)) {
        foreach ($rdMsg->headers as $k => $v) {
            $headers[(string) $k] = (string) $v;
        }
    }

    $headers['x-original-topic'] = (string) $rdMsg->topic_name;
    $headers['x-original-partition'] = (string) $rdMsg->partition;
    $headers['x-original-offset'] = (string) $rdMsg->offset;

    return new Message(
        payload: (string) $rdMsg->payload,
        headers: $headers,
        key: $rdMsg->key !== null ? (string) $rdMsg->key : null,
        partition: (int) $rdMsg->partition,
        timestampMs: (int) ($rdMsg->timestamp ?? 0),
    );
}
```

**关键**：

- 把 librdkafka 的 `headers` 数组（结构不一致，可能是 array / object）归一化成 `array<string,string>`
- 注入 3 个 Kafka 侧 header：
  - `x-original-topic` —— 源 topic
  - `x-original-partition` —— 源 partition
  - `x-original-offset` —— 源 offset
- 这些 header 在 DLQ 消息里用于定位原始位置

## 4.7 `src/Consumer/ConsumerFactory.php` —— 工厂与 rebalance

**作用**：单例缓存 Consumer，配置 rebalance 回调。

```php
public function make(KafkaConfig $config, ?Subscription $subscription = null): Consumer
{
    $key = $config->name();
    if (! isset($this->instances[$key])) {
        $subscription = $subscription ?? new Subscription([$config->defaultTopic()]);
        $this->instances[$key] = $this->build($config, $subscription);
    }
    return $this->instances[$key];
}
```

**`make($config, $subscription = null)`**：

- 第一个调用：传 `$subscription`（来自 WorkCommand 的 `--queue` 参数）
- 后续调用：忽略 `$subscription`，复用已有 Consumer（因为 Consumer 已 subscribe 过，再改订阅需要 new）

**`$subscription ?? new Subscription([$config->defaultTopic()])`**：

- 兜底：没传 subscription 时，默认订阅 `defaultTopic`
- 配合 WorkCommand `kafka:work`（不传 `--queue` 时的默认行为）

### 4.7.1 `rebalance` 回调

```php
$conf->setRebalanceCb(function (RdKafkaConsumer $kafka, $err, ?array $partitions = null) {
    switch ($err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            $kafka->assign($partitions);
            error_log(sprintf(
                '[laravel-kafka] rebalance: assigned %d partition(s)',
                is_array($partitions) ? count($partitions) : 0
            ));
            break;
        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            $kafka->assign(null);
            error_log('[laravel-kafka] rebalance: revoked partitions');
            break;
        default:
            $kafka->assign(null);
            error_log(sprintf('[laravel-kafka] rebalance: error code=%d', $err));
    }
});
```

**librdkafka rebalance 协议铁律**：

- 每次 rebalance 必须**手动**调 `assign(partitions)` 或 `assign(null)`，否则 Consumer 不读不写
- **ASSIGN** 事件（新分配 partition）→ `assign($partitions)` 开始消费
- **REVOKE** 事件（被剥夺 partition）→ `assign(null)` 停止消费 + 提交 offset
- 其他错误 → `assign(null)` 安全退出

**为什么必须手动 assign**：

- librdkafka 设计上让用户在 rebalance 时检查状态、做清理
- 自动 assign 会让"清理未完成事务"等场景不可控

**v0.1 简化为只 log**：

- 真正接 Laravel 事件（`kafka.rebalance.assigned` / `kafka.rebalance.revoked`）是 v0.2
- 当前只 log 供调试

## 4.8 Step 4 小结

| 类 | 职责 | 关键设计 |
| --- | --- | --- |
| `Subscription` | topic 列表值对象 | 至少 1 个 + 去重 + 强转 string |
| `HandlerInterface` | handler 契约 | 单方法 `handle(Message): HandlerResult` |
| `HandlerResult` | 三态结果 | ack / requeue / dlq，类常量模拟 enum |
| `HandlerResolver` | handler 路由 | v0.1 单路，v0.3 多路（接口已留好） |
| `NativeHandler` | Laravel Job 桥 | 调 `Worker::process` + 失败决策树 |
| `Consumer` | librdkafka 封装 | poll/ack/close + wrap 注入原始 header |
| `ConsumerFactory` | 单例缓存 + rebalance | setRebalanceCb 必须手动 assign |

**关键认知**：

1. **`HandlerResult` 三态**是消费者侧决策的核心：ack / requeue / dlq
2. **`NativeHandler` 完全复用 Laravel `Worker::process`**：不重写 Laravel 内部逻辑，扩展失败处理边界
3. **`setRebalanceCb` 必须手动 assign**：librdkafka 的铁律，否则 Consumer 不工作
4. **`Consumer::wrap` 注入原始 header**：让 DLQ 消息能定位到原始位置

---

# Step 5 精读：Queue 子系统

**目标**：让 Laravel Queue 框架"以为"这是个普通 Queue 驱动。

3 个文件：`KafkaConnector` / `KafkaQueue` / `KafkaJob`

## 5.1 `src/Queue/KafkaConnector.php` —— 框架识别入口

**作用**：实现 `Illuminate\Queue\Connectors\ConnectorInterface`，Laravel 通过 `Queue::extend('kafka', ...)` 拿到这个类。

```php
final class KafkaConnector implements ConnectorInterface
{
    private KafkaManager $manager;

    public function __construct(KafkaManager $manager)
    {
        $this->manager = $manager;
    }

    public function connect(array $config): KafkaQueue
    {
        $name = (string) ($config['name'] ?? 'default');
        $queue = $this->manager->connection($name);
        if (! $queue instanceof KafkaQueue) {
            throw new \RuntimeException(sprintf(
                'Kafka connection [%s] is not a KafkaQueue instance (got %s).',
                $name,
                is_object($queue) ? get_class($queue) : gettype($queue)
            ));
        }
        return $queue;
    }
}
```

**关键设计**：

### 5.1.1 单方法 `connect(array $config)`

- Laravel 调 `connect()` 拿一个 `Queue` 实例
- `$config` 来自 `config/queue.php` 的 `connections.kafka` 数组
- 我们只读 `name`（connection 名），其他 Kafka 配置从 `config/kafka.php` 拿——**两套 config 不重复**

### 5.1.2 防御性类型校验

```php
if (! $queue instanceof KafkaQueue) {
    throw new \RuntimeException(...);
}
```

- 理论上 `Manager::connection()` 一定返回 `KafkaQueue`
- 但 `Manager` 是个 hook，可能被扩展覆盖返回其他类型
- 显式断言避免上层拿到错误类型不感知

**为什么不返回 `Queue` 接口**：

- 实现签名 `connect(array $config): KafkaQueue` 而不是 `... Queue`——把 `KafkaQueue` 当返回类型
- 强制业务层用 Kafka 特有的方法（如 `failedHandler()`）时不用 instanceof

## 5.2 `src/Queue/KafkaQueue.php` —— 核心 Queue 实现

**作用**：继承 `Illuminate\Queue\Queue`，实现 Laravel 队列的所有标准方法（push / pushRaw / later / pop / size）。

### 5.2.1 构造函数的关键细节

```php
public function __construct(
    Producer $producer,
    Consumer $consumer,
    FailedJobHandlerInterface $failedHandler,
    KafkaConfig $config,
    string $connectionName
) {
    parent::__construct(null);
    $this->producer = $producer;
    $this->consumer = $consumer;
    $this->failedHandler = $failedHandler;
    $this->config = $config;
    $this->connectionName = $connectionName;
}
```

**`parent::__construct(null)`**：

- `Illuminate\Queue\Queue` 基类构造器期望 `Container $container`
- 我们传 `null`——因为 `ConnectionFactory::make` 时容器可能还没准备好
- 之后用 `setContainer()` 延迟注入

```php
public function setContainer(\Illuminate\Contracts\Container\Container $container): void
{
    $this->container = $container;
}
```

**`setContainer()` 的存在意义**：

- Laravel 启动后期会调 `$queue->setContainer($app)` 把容器塞进来
- 我们的 KafkaQueue 需要容器来解析 Job handler 类
- 显式 `setContainer` 让构造器不必依赖容器（顺序解耦）

### 5.2.2 `size()` 返回 0

```php
public function size($queue = null): int
{
    return 0;
}
```

**为什么返回 0**：

- Kafka 没有"队列长度"概念
- "队列长度"需要查 `__consumer_offsets` + `high_watermark` 估算
- v0.1 不实现——用户跑 `php artisan queue:size` 会看到 0
- v0.2 接入 Kafka admin API 实现

**为什么不抛 `RuntimeException`**：

- Laravel 某些命令（如 `queue:monitor`）会调 `size()`
- 抛异常会让这些命令挂掉
- 返回 0 让命令至少不崩（虽然数据不准）

### 5.2.3 `pop()` 返回 null

```php
public function pop($queue = null)
{
    return null;
}
```

**为什么返回 null**：

- 真正的 pop 发生在 `kafka:work` 长进程的 poll 循环里
- Laravel 默认的 `queue:work` 会调 `pop()` 拉消息——但 Kafka 模型与 Laravel 默认驱动（Redis / database）不兼容
- v0.1 设计：**禁止**用 `queue:work`，必须用 `kafka:work`
- `pop()` 返回 null 让 Laravel 默认 worker 拿到 null 直接退出

**为什么不在 v0.1 适配 `queue:work`**：

- Laravel `queue:work` 的循环模型（while pop != null）是 Redis 风格的轮询
- Kafka 风格的"长驻 poll"完全不一样
- 适配需要重写 Laravel `Worker` 类的循环逻辑
- v0.1 不做，留到 v0.2 评估

### 5.2.4 `push()` / `pushRaw()` / `later()` —— 业务核心

```php
public function push($job, $data = '', $queue = null)
{
    return $this->pushRaw(
        $this->createPayload($job, $this->connectionName, $data, $queue),
        $queue,
        []
    );
}

public function pushRaw($payload, $queue = null, array $options = [])
{
    $topic = $this->resolveTopic($queue);
    $message = $this->buildMessage($payload, $options, $queue);
    return $this->producer->send($topic, $message);
}

public function later($delay, $job, $data = '', $queue = null)
{
    return $this->pushRaw(
        $this->createPayload($job, $this->connectionName, $data, $queue),
        $queue,
        ['delay_seconds' => max(0, (int) $delay)]
    );
}
```

**关键设计**：

- `push($job, $data, $queue)`：Laravel 业务代码写 `Queue::push(new MyJob())`，调到这里
- `$this->createPayload(...)` —— **基类方法**，把 Job + data 序列化成 PHP 字符串 payload
- `$this->pushRaw(...)` —— 把 payload 投到 Kafka
- `later($delay, ...)` —— 与 push 一致，但 options 多一个 `delay_seconds`

**为什么复用 `createPayload`**：

- Laravel `Illuminate\Queue\Queue` 基类有 `createPayload` / `createPayloadArray` / `createPayloadString` 三个方法
- 我们不重写——自动得到与 Laravel 兼容的 payload 结构
- payload 包含 `uuid` / `displayName` / `job` / `data.commandName` / `data.command` 等字段
- 消费端的 `CallQueuedHandler@call` 能直接解析

### 5.2.5 `buildMessage()` 注入 5 个基础 header

```php
private function buildMessage(string $payload, array $options, ?string $queue): Message
{
    $now = (int) (microtime(true) * 1000);

    $headers = [
        Header::TRACE_ID => bin2hex(random_bytes(8)),
        Header::QUEUE => (string) ($queue ?? $this->config->defaultTopic()),
        Header::CONNECTION => $this->connectionName,
        Header::ENQUEUED_AT => (string) $now,
        Header::RETRY_COUNT => '0',
        Header::SERIALIZER => 'php',
    ];

    if (isset($options['delay_seconds']) && (int) $options['delay_seconds'] > 0) {
        $headers[Header::AVAILABLE_AT] = (string) ($now + ((int) $options['delay_seconds']) * 1000);
    }

    return new Message(
        payload: $payload,
        headers: $headers,
        key: isset($options['key']) ? (string) $options['key'] : null,
    );
}
```

**5 个基础 header**：

| Header | 用途 | 来源 |
| --- | --- | --- |
| `x-trace-id` | 调用链追踪 | 16 字符十六进制（random_bytes(8)） |
| `x-queue` | 消费端还原队列名 | $queue 或 defaultTopic |
| `x-connection` | 消费端识别 connection | $connectionName |
| `x-enqueued-at` | 入队时间戳 | microtime ms |
| `x-attempt` | 重试计数 | 0（首次） |
| `x-serializer` | 序列化器标识 | "php"（v0.1） |

**延迟消息的 `x-available-at`**：

- 计算 `now + delay_seconds * 1000`
- 消费端 NativeHandler 检查：`if (availableAt > now)` → 当前不能消费

**`key` 选项**：

- `options['key']` 是 v0.1 给 `pushRaw` 透传的入口
- v0.1 没暴露给业务方（`Queue::push()` 不支持 key）
- v0.2 暴露 `KafkaQueue::push($job, $data, $queue, $key)`

## 5.3 `src/Queue/KafkaJob.php` —— Laravel Job 包装

**作用**：继承 `Illuminate\Queue\Jobs\Job`，把 Kafka 消息包装成 Laravel Job 对象，让 `Worker::process` 能用。

### 5.3.1 构造

```php
public function __construct(
    Container $container,
    Consumer $consumer,
    \RdKafka\Message $rdMessage,
    string $connectionName,
    string $queue
) {
    $this->container = $container;
    $this->consumer = $consumer;
    $this->rdMessage = $rdMessage;
    $this->connectionName = $connectionName;
    $this->queue = $queue;
    $this->rawBody = (string) $rdMessage->payload;
    $this->headers = $this->normalizeHeaders($rdMessage->headers);
}
```

**5 个入参的设计动机**：

| 入参 | 用途 |
| --- | --- |
| `Container` | 解析 Job handler 类、FailedJobHandler |
| `Consumer` | `ack()` 时 commit offset |
| `RdKafka\Message` | 拿原始 partition / offset / topic_name |
| `connectionName` | Laravel `Worker` 识别当前 connection |
| `queue` | Laravel Job 的逻辑队列名 |

**为什么绕过 `parent::__construct`**：

- `Illuminate\Queue\Jobs\Job` 父类构造器是 `(Container, $connectionName, $queue, $rawBody)`
- 我们的入参**不完全一致**——多了 `Consumer` 和 `RdKafka\Message`
- 显式赋值各属性，绕过父类构造器

### 5.3.2 `getJobId()`

```php
public function getJobId(): ?string
{
    $jobId = $this->headers[Header::JOB_ID] ?? null;
    if ($jobId !== null) {
        return $jobId;
    }
    return (string) $this->rdMessage->offset;
}
```

**优先级**：

1. 优先用 `x-job-id` header（push 时显式注入）
2. 兜底用 `RdKafka\Message::offset`（partition 内偏移）

**为什么用 offset 兜底**：

- 即使业务没显式 `x-job-id`，每条 Kafka 消息都有唯一 offset
- offset 在 partition 内是唯一 ID；跨 partition 不唯一（这在 distributed 场景下没问题）
- 业务方一般用 `Job::getJobId()` 写日志 / 标记，offset 已经够用

### 5.3.3 `attempts()`

```php
public function attempts(): int
{
    $count = $this->headers[Header::RETRY_COUNT] ?? '0';
    return (int) $count + 1;
}
```

**为什么 `+ 1`**：

- Laravel 语义：`attempts() === 1` 表示"首次执行"，`=== 2` 表示"第一次重试"
- `RETRY_COUNT` header 记录"已经重试了几次"——首次时是 0
- 1-indexed 转换：`attempts = retry_count + 1`

### 5.3.4 `delete()` 提交 offset

```php
public function delete()
{
    parent::delete();
    $this->consumer->ack($this->rdMessage);
}
```

**关键**：业务成功后 `$job->delete()` 会调到：

1. 父类 `delete()` 标记 `deleted = true`
2. 我们额外 `consumer->ack($rdMessage)` commit offset
3. **两步缺一不可**——父类标志让 Worker 知道不重试，commit 让 Kafka 知道下次不重发

### 5.3.5 `fail($exception)` 走 FailedJobHandler

```php
public function fail($exception)
{
    $this->markAsFailed();

    if ($exception instanceof Throwable) {
        $handler = $this->container->make(\LaravelKafka\Queue\Failed\FailedJobHandlerFactory::class)
            ->makeFor($this->container->make('kafka.manager')->config());

        $handler->handle($this, $exception, new FailedContext(
            $this->rawBody,
            $this->headers,
            (string) ($this->rdMessage->topic_name ?? 'laravel-jobs'),
            (int) ($this->rdMessage->partition ?? 0),
            $this->attempts() - 1,
        ));
    }

    $this->consumer->ack($this->rdMessage);
}
```

**逐段解释**：

1. `markAsFailed()` —— 用反射把父类的 `$failed` 标志设为 true
2. 从容器拿 `FailedJobHandlerFactory` 和 `KafkaConfig`
3. 构造 `FailedContext`（payload / headers / topic / partition / attempts）
4. `$handler->handle(...)` —— 写失败（database 表 / DLQ topic / 双写）
5. `$consumer->ack($this->rdMessage)` —— 提交 offset，消息从主 topic 视角移除

**为什么反射**：

- 父类 `Illuminate\Queue\Jobs\Job::$failed` 是 `protected`
- PHP 7.4 没"外部访问父类 protected"语法
- 用 `ReflectionClass` + `setAccessible(true)` 强行写

```php
private function markAsFailed(): void
{
    $reflection = new \ReflectionClass(parent::class);
    if ($reflection->hasProperty('failed')) {
        $prop = $reflection->getProperty('failed');
        $prop->setAccessible(true);
        $prop->setValue($this, true);
    }
}
```

**已知风险**（来自开发日志 §16.5 已知待办）：

- Laravel 8/9/10/11 父类 `$failed` 字段名可能变化
- v0.2 应改成"重写 `markAsFailed()` 的语义"，绕开反射

### 5.3.6 `release($delay)` 不直接 produce

```php
public function release($delay = 0)
{
    parent::release($delay);
    // 实际上 release 由 NativeHandler 通过 HandlerResult::requeue() 路径处理
    // 这里不直接 produce，避免双重入队
}
```

**为什么不在这里 produce**：

- Laravel 的 `Job::release()` 是 Laravel 抽象层概念
- Kafka 的 requeue 是 Producer.send + 增加 retry_count header
- 完整 requeue 逻辑在 `NativeHandler::onException()` 走 `HandlerResult::requeue()` 路径
- 这里只让父类记录"已 release"状态，**不**做实际 produce

## 5.4 Step 5 小结

| 类 | 职责 | 关键设计 |
| --- | --- | --- |
| `KafkaConnector` | Laravel 框架识别入口 | 单 `connect()` 方法 + 防御性类型校验 |
| `KafkaQueue` | Laravel Queue 契约实现 | 继承基类 + 复用 createPayload + 5 个基础 header |
| `KafkaJob` | Kafka 消息的 Laravel 包装 | 反射写 failed + ack 提交 offset |

**关键认知**：

1. **`parent::__construct(null)` + `setContainer()` 延迟注入**：让构造器不必依赖容器
2. **`pop()` 返回 null**：禁止用 `queue:work`，必须用 `kafka:work`
3. **`size()` 返回 0**：v0.1 不实现 Kafka admin API
4. **`createPayload` 复用基类**：自动得到 Laravel 兼容的 payload 结构
5. **5 个基础 header**：在 `pushRaw` 时注入，让消费端能还原上下文
6. **`markAsFailed` 用反射**：侵入式但 PHP 7.4 没别的办法

---

# Step 6 精读：Failed 子系统

**目标**：实现三种失败处理模式——database / dlq / hybrid。

6 个文件：`FailedJobHandlerInterface` / `FailedContext` / `FailedJobHandlerFactory` / 3 个 Handler 实现

## 6.1 `src/Queue/Failed/FailedJobHandlerInterface.php` —— 失败处理契约

**作用**：定义"业务失败时怎么处理"的标准接口。

```php
interface FailedJobHandlerInterface
{
    public function handle(KafkaJob $job, Throwable $exception, FailedContext $context): void;
}
```

**为什么是单方法接口**：

- 失败处理是单一动作，没有状态机
- 实现类只关心"把失败信息写到哪里"
- 测试时 mock 一个 FailedHandler 极简

**3 个入参的设计动机**：

| 入参 | 用途 |
| --- | --- |
| `KafkaJob $job` | 拿 connection / queue / payload / header |
| `Throwable $exception` | 失败原因（class / message / trace） |
| `FailedContext $context` | Kafka 侧元信息（topic / partition / attempts） |

**为什么 3 个入参而不是 1 个值对象**：

- `FailedContext` 已经打包了 Kafka 侧元信息
- `KafkaJob` 是 Job 视角的（用于写 `failed_jobs` 表时填 `queue` / `connection`）
- `Throwable` 单独传——异常对象语义独立

## 6.2 `src/Queue/Failed/FailedContext.php` —— 失败上下文

**作用**：把 Kafka 侧的失败元信息打包。

```php
final class FailedContext
{
    public function __construct(
        private string $rawPayload,
        private array $headers,
        private string $topic,
        private int $partition,
        private int $attempts,
    ) {
    }

    public function rawPayload(): string { return $this->rawPayload; }
    public function headers(): array { return $this->headers; }
    public function topic(): string { return $this->topic; }
    public function partition(): int { return $this->partition; }
    public function attempts(): int { return $this->attempts; }
}
```

**5 个字段**：

| 字段 | 用途 |
| --- | --- |
| `rawPayload` | 原始消息体（不重新序列化） |
| `headers` | 原始 Kafka headers |
| `topic` | 源 topic |
| `partition` | 源 partition |
| `attempts` | 已重试次数（0 = 首次失败） |

**为什么 `attempts` 是 0-indexed 而 `Job::attempts()` 是 1-indexed**：

- 这里的 `attempts` 表示"已经重试过几次"——首次失败时是 0
- 业务方读 `Job::attempts()` 拿到的是 1（首次）
- 转换在 `KafkaJob::fail()` 里做：`new FailedContext(..., $this->attempts() - 1)`

## 6.3 `src/Queue/Failed/FailedJobHandlerFactory.php` —— 工厂

**作用**：按 `failed.driver` 配置路由到 3 种 Handler 实现。

### 6.3.1 缓存与路由

```php
final class FailedJobHandlerFactory
{
    private Container $container;
    private array $instances = [];

    public function make(KafkaConfig $config): FailedJobHandlerInterface
    {
        $key = $config->name();
        if (isset($this->instances[$key])) {
            return $this->instances[$key];
        }

        $driver = (string) ($config->failed()['driver'] ?? 'hybrid');

        switch ($driver) {
            case 'database':
                $this->instances[$key] = $this->makeDatabase($config);
                break;
            case 'dlq':
                $this->instances[$key] = $this->makeDlq($config);
                break;
            case 'hybrid':
                $this->instances[$key] = $this->makeHybrid($config);
                break;
            default:
                throw new KafkaException(sprintf(
                    'Invalid failed.driver "%s". Allowed: database, dlq, hybrid.',
                    $driver
                ));
        }

        return $this->instances[$key];
    }
    ...
}
```

**关键设计**：

- `instances[$config->name()]` 缓存——同一 connection 只 new 一次
- `switch ($driver)` 路由
- 未知 driver 抛 `KafkaException`——配置错误要立刻暴露

### 6.3.2 DLQ topic 自动拼接

```php
private function resolveDlqTopic(KafkaConfig $config, array $dlqConfig): string
{
    $explicit = (string) ($dlqConfig['topic'] ?? '');
    if ($explicit !== '') {
        return $explicit;
    }
    $suffix = (string) ($dlqConfig['auto_topic_suffix'] ?? '.dlq');
    return $config->defaultTopic() . $suffix;
}
```

**规则**：

1. 用户在 `failed.dlq.topic` 显式配 → 用它
2. 否则用 `defaultTopic + auto_topic_suffix`（默认 `.dlq`）

**示例**：

- 默认 topic `laravel-jobs` → DLQ `laravel-jobs.dlq`
- 默认 topic `orders` → DLQ `orders.dlq`
- 用户配 `failed.dlq.topic = 'my-dlq'` → 全部失败进 `my-dlq`

### 6.3.3 `makeDatabase()`

```php
private function makeDatabase(KafkaConfig $config): DatabaseFailedJobHandler
{
    $dbConfig = (array) ($config->failed()['database'] ?? []);
    $table = (string) ($dbConfig['table'] ?? 'failed_jobs');
    $connection = $dbConfig['connection'] ?? null;

    $database = $this->container->make(ConnectionResolverInterface::class)
        ->connection($connection);

    return new DatabaseFailedJobHandler($database, $table, new Uuid());
}
```

**关键**：

- 从容器拿 `ConnectionResolverInterface`（Laravel 抽象的数据库连接解析器）
- 注入 `Uuid()`（Ramsey 工厂实例）—— v0.1.1 修补：从 ServiceProvider 改为从容器解析（开发日志 §16.7 Step 7 修补）

### 6.3.4 `makeDlq()`

```php
private function makeDlq(KafkaConfig $config): DlqFailedJobHandler
{
    $dlqConfig = (array) ($config->failed()['dlq'] ?? []);
    $producer = $this->container->make(ProducerFactory::class)->make($config);

    return new DlqFailedJobHandler(
        $producer,
        $this->resolveDlqTopic($config, $dlqConfig),
        $dlqConfig,
    );
}
```

**`producer` 注入**：

- DLQ handler 写消息到 DLQ topic 需要 producer
- 用的是同一个 connection 的 producer（缓存命中，无新连接）
- `ProducerFactory::make($config)` 会从缓存拿现成的 producer

### 6.3.5 `makeHybrid()`

```php
private function makeHybrid(KafkaConfig $config): HybridFailedJobHandler
{
    $hybridConfig = (array) ($config->failed()['hybrid'] ?? []);

    return new HybridFailedJobHandler(
        $this->makeDatabase($config),
        $this->makeDlq($config),
        (int) ($hybridConfig['max_attempts'] ?? 3),
        (array) ($hybridConfig['fatal_exceptions'] ?? []),
        (int) ($hybridConfig['trace_truncate_bytes'] ?? 32768),
        (int) ($hybridConfig['message_truncate_bytes'] ?? 4096),
    );
}
```

**Hybrid 包装 database + dlq**：

- **不是"先 database 后 dlq"**——是同时给 Hybrid 决策
- 内部 HybridFailedJobHandler 自己决策写哪个 / 都写

## 6.4 `src/Queue/Failed/DatabaseFailedJobHandler.php` —— 写 failed_jobs 表

**作用**：把失败任务写入 `failed_jobs` 表，结构与 Laravel 标准一致。

### 6.4.1 表结构

```sql
CREATE TABLE failed_jobs (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    uuid            VARCHAR(36) UNIQUE,
    connection      VARCHAR(255),
    queue           VARCHAR(255),
    payload         LONGTEXT,
    exception       LONGTEXT,
    failed_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**为什么是 Laravel 8+ 的 `DatabaseUuidFailedJobProvider` 兼容 schema**：

- Laravel `queue:failed` / `queue:retry` 命令读这个 schema
- 我们自己写表，但**不**让 Laravel 命令失效
- `queue.failer.kafka` 容器键 + `syncFailedTableConfig` 桥接（见 Step 2）

### 6.4.2 `handle()` 实现

```php
public function handle(KafkaJob $job, Throwable $exception, FailedContext $context): void
{
    $uuid = (string) $this->uuidFactory->toString();
    $payload = $this->encodePayload($job);
    $exceptionText = $this->encodeException($exception);

    $this->database->table($this->table)->insert([
        'uuid' => $uuid,
        'connection' => $job->getConnectionName(),
        'queue' => $job->getQueue(),
        'payload' => $payload,
        'exception' => $exceptionText,
        'failed_at' => date('Y-m-d H:i:s'),
    ]);
}
```

**逐字段解释**：

| 字段 | 写入内容 | 备注 |
| --- | --- | --- |
| `uuid` | Ramsey UUID | 业务方重试用 `queue:retry <uuid>` |
| `connection` | `$job->getConnectionName()` | `kafka` |
| `queue` | `$job->getQueue()` | Laravel 逻辑队列名 |
| `payload` | `encodePayload($job)` | 见下文 |
| `exception` | `encodeException($exception)` | 见下文 |
| `failed_at` | `date('Y-m-d H:i:s')` | 当前时间 |

### 6.4.3 `encodePayload()`

```php
private function encodePayload(KafkaJob $job): string
{
    $payload = [
        'uuid' => (string) $job->getJobId(),
        'displayName' => $job->resolveName(),
        'job' => 'Illuminate\\Queue\\CallQueuedHandler@call',
        'maxTries' => null,
        'maxExceptions' => null,
        'backoff' => null,
        'timeout' => null,
        'retryUntil' => null,
        'data' => [
            'commandName' => $job->getName(),
            'command' => $job->getRawBody(),
        ],
    ];
    return json_encode($payload, JSON_UNESCAPED_UNICODE);
}
```

**关键**：

- 复刻 Laravel 8+ `DatabaseUuidFailedJobProvider` 的 payload 结构
- `data.command` 是 `php serialize` 的字符串（不是 JSON）
- `data.commandName` 是 Job 类的全限定名

**已知风险**（开发日志 §16.6 待办）：

- Laravel 9/10/11 的 `data.command` 字段是对象（不是字符串）
- v0.1.1 加跨版本 fixture 测试验证

### 6.4.4 `encodeException()`

```php
private function encodeException(Throwable $exception): string
{
    return implode("\n", [
        get_class($exception),
        $exception->getMessage(),
        $exception->getTraceAsString(),
    ]);
}
```

**`implode("\n", ...)`**：

- Laravel 默认 `failed_jobs.exception` 是这种格式
- 业务方查表时人眼可读
- `getTraceAsString()` 可能很长——hybrid 模式下 HybridFailedJobHandler 会先按 `trace_truncate_bytes` 截断再传进来

## 6.5 `src/Queue/Failed/DlqFailedJobHandler.php` —— 写 DLQ topic

**作用**：把失败任务写入 DLQ topic，注入 9 个 DLQ 专属 header。

### 6.5.1 `handle()` 注入 9 个 header

```php
public function handle(KafkaJob $job, Throwable $exception, FailedContext $context): void
{
    $messageTruncate = (int) ($this->config['message_truncate_bytes'] ?? 4096);
    $traceTruncate = (int) ($this->config['trace_truncate_bytes'] ?? 32768);

    $headers = array_merge(
        $context->headers(),
        [
            'x-original-topic' => $context->topic(),
            'x-original-partition' => (string) $context->partition(),
            'x-original-headers' => json_encode(
                $context->headers(),
                JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES
            ),
            'x-failed-at' => (string) (int) (microtime(true) * 1000),
            'x-exception-class' => get_class($exception),
            'x-exception-message' => $this->truncate($exception->getMessage(), $messageTruncate),
            'x-exception-trace' => $this->truncate($exception->getTraceAsString(), $traceTruncate),
            'x-attempts' => (string) $context->attempts(),
            'x-job-id' => (string) $job->getJobId(),
            'x-queue' => $job->getQueue(),
            'x-connection' => $job->getConnectionName(),
        ]
    );

    $message = new Message(
        payload: $context->rawPayload(),
        headers: $headers,
        key: $context->headers()[Header::TRACE_ID] ?? null,
    );

    $this->producer->send($this->dlqTopic, $message);
}
```

**9 个 DLQ 专属 header**：

| Header | 含义 |
| --- | --- |
| `x-original-topic` | 源 topic |
| `x-original-partition` | 源 partition |
| `x-original-offset` | 源 offset（在 `Consumer::wrap` 已注入） |
| `x-original-headers` | 原 headers JSON 序列化 |
| `x-failed-at` | 失败时间（ms） |
| `x-exception-class` | 异常类名 |
| `x-exception-message` | 异常 message（截断 4KB） |
| `x-exception-trace` | 异常 trace（截断 32KB） |
| `x-attempts` | 累计重试次数 |

**payload 不重新序列化**：

- `$context->rawPayload()` 原样透传
- 跨语言消费 DLQ 的用户能直接拿到原始 payload
- 例：Node consumer 拿到 PHP serialize 的 payload，需要 PHP consumer 解码

**`key` 用 `x-trace-id`**：

- DLQ 消息的 key = 原消息的 trace_id
- 让所有"同一 trace"的失败消息落同一 partition
- 下游分析可以按 trace 聚合失败

### 6.5.2 `truncate()`

```php
private function truncate(string $value, int $maxBytes): string
{
    if (strlen($value) <= $maxBytes) {
        return $value;
    }
    return substr($value, 0, $maxBytes) . '... [truncated]';
}
```

**为什么按字节截断而不是按字符**：

- Kafka header 是字节流（librdkafka 层）
- librdkafka 限制 header 总大小（默认 1MB）
- 大 trace 可能撑爆 header 限制

**已知缺陷**（开发日志 §16.6 待办）：

- 用 `strlen` + `substr` 字节级截断，对中文 / emoji 可能产生半个字
- v0.2 用 `mb_strcut` 字符级截断

## 6.6 `src/Queue/Failed/HybridFailedJobHandler.php` —— 决策树

**作用**：根据异常类型和重试次数，决定写 database / DLQ / 都写。

### 6.6.1 决策树

```php
public function handle(KafkaJob $job, Throwable $exception, FailedContext $context): void
{
    $isFatal = $this->isFatal($exception);
    $attemptNumber = $context->attempts() + 1; // 1-indexed
    $overLimit = $attemptNumber >= $this->maxAttempts;

    if ($isFatal || $overLimit) {
        // 双写：database + DLQ
        $dbError = null;
        try {
            $this->database->handle($job, $exception, $context);
        } catch (Throwable $e) {
            $dbError = $e;
        }
        try {
            $this->dlq->handle($job, $exception, $context);
        } catch (Throwable $e) {
            if ($dbError !== null) {
                throw new \RuntimeException(
                    'Hybrid handler: both database and DLQ writes failed',
                    0,
                    $e
                );
            }
            throw $e;
        }
        if ($dbError !== null) {
            error_log('[laravel-kafka] hybrid handler: database write failed but DLQ succeeded: ' . $dbError->getMessage());
        }
        return;
    }

    // 未到 max_attempts 且非致命：仅写 database
    $this->database->handle($job, $exception, $context);
}
```

**两种场景**：

#### 场景 A：致命异常 OR 达 max_attempts

- **同时**写 database + DLQ
- 任一失败用 try/catch 隔离
- 如果两边都失败 → 抛 `RuntimeException`（让上层 Worker 知道）

#### 场景 B：未致命且未超限

- 仅写 database（重试用，DLQ 留到下次）

### 6.6.2 `isFatal()`

```php
private function isFatal(Throwable $e): bool
{
    foreach ($this->fatalExceptions as $class) {
        if ($e instanceof $class) {
            return true;
        }
    }
    return false;
}
```

**`instanceof` 检查**：

- `SerializationException`、`ValidationException` 默认在 fatal list
- 用户可在 `failed.hybrid.fatal_exceptions` 加自定义类
- `instanceof` 支持继承匹配

### 6.6.3 双写失败的处理

```php
$dbError = null;
try {
    $this->database->handle($job, $exception, $context);
} catch (Throwable $e) {
    $dbError = $e;
}
try {
    $this->dlq->handle($job, $exception, $context);
} catch (Throwable $e) {
    if ($dbError !== null) {
        throw new \RuntimeException(
            'Hybrid handler: both database and DLQ writes failed',
            0,
            $e
        );
    }
    throw $e;
}
if ($dbError !== null) {
    error_log('[laravel-kafka] hybrid handler: database write failed but DLQ succeeded: ' . $dbError->getMessage());
}
```

**3 种组合**：

| database | DLQ | 行为 |
| --- | --- | --- |
| 成功 | 成功 | 静默 |
| 成功 | 失败 | 抛 DLQ 的异常（让 Worker 知道） |
| 失败 | 成功 | log 一行（DLQ 是真理之源，不阻塞） |
| 失败 | 失败 | 抛 RuntimeException 包装 |

**为什么"DLQ 失败时阻塞，database 失败时不阻塞"**：

- DLQ 是"最后的兜底"——如果连 DLQ 都写不进去，消息就真的丢了
- 必须让 Worker 知道，让消费者 retry offset
- database 写失败**还有 DLQ**，业务可查 DLQ
- 静默 log 不让一条 database 写入失败影响消费流程

## 6.7 Step 6 小结

| 类 | 职责 | 关键设计 |
| --- | --- | --- |
| `FailedJobHandlerInterface` | 失败处理契约 | 单方法 handle + 3 入参 |
| `FailedContext` | 失败上下文值对象 | payload/headers/topic/partition/attempts |
| `FailedJobHandlerFactory` | 按 driver 路由 | database/dlq/hybrid + DLQ topic 自动拼接 |
| `DatabaseFailedJobHandler` | 写 failed_jobs 表 | 复刻 Laravel 8+ schema |
| `DlqFailedJobHandler` | 写 DLQ topic | 9 个 DLQ 专属 header + payload 透传 |
| `HybridFailedJobHandler` | 决策树 | 致命 OR 超限双写，其他单写 database |

**关键认知**：

1. **三模式可配**是用户决议（§11.1）：database / dlq / hybrid，默认 hybrid
2. **DLQ 消息 payload 不重新序列化**：跨语言消费友好
3. **Hybrid 双写失败时 DLQ 优先**：DLQ 是真理之源，database 失败静默
4. **复刻 Laravel schema** 是为了让 `queue:failed` / `queue:retry` 命令继续工作

---

# Step 7 精读：Console 命令 & ServiceProvider 桥接

**目标**：实现 `kafka:work` 长驻 worker，并补 Step 6 提到的 ServiceProvider 桥接。

1 个新文件 + ServiceProvider 修补

## 7.1 `src/Console/WorkCommand.php` —— kafka:work 实现

### 7.1.1 命令签名

```php
protected $signature = 'kafka:work
    {--queue=* : 订阅的 topic 列表（可多次指定，逗号分隔）}
    {--connection=default : Kafka 连接名}
    {--max-time=0 : 最大运行秒数，0 = 不限}
    {--max-jobs=0 : 最大处理任务数，0 = 不限}
    {--sleep=1 : 无消息时的 sleep 秒数}';
```

**5 个选项**：

| 选项 | 默认 | 用途 |
| --- | --- | --- |
| `--queue` | `[]` | 多次指定或逗号分隔 |
| `--connection` | `default` | 多 connection 场景 |
| `--max-time` | 0 | 限时间（k8s liveness 用） |
| `--max-jobs` | 0 | 限次数（cron 一次性跑批） |
| `--sleep` | 1 | 无消息 sleep 秒（避免 CPU 空转） |

**为什么 `--queue=*` 用数组**：

- Laravel 命令的 `*` 后缀让参数支持多值
- 用法：`--queue=emails --queue=reports` 或 `--queue=emails,reports`

### 7.1.2 主循环

```php
while (! $this->shouldQuit) {
    if ($maxTime > 0 && (time() - $startTime) >= $maxTime) {
        $this->info('[kafka:work] max-time reached, exiting');
        break;
    }
    if ($maxJobs > 0 && $jobCount >= $maxJobs) {
        $this->info('[kafka:work] max-jobs reached, exiting');
        break;
    }

    $message = $consumer->poll(1000);
    if ($message === null) {
        if ($sleep > 0) {
            sleep($sleep);
        }
        continue;
    }

    $this->processMessage($resolver, $consumer, $message);
    $jobCount++;
}
```

**循环控制**：

- `shouldQuit` 由信号处理函数置 true
- 每次循环开头检查 max-time / max-jobs
- `poll(1000)` —— 1 秒超时，能及时响应信号
- 无消息时 `sleep($sleep)` 减 CPU 占用

**`poll(1000)` 为什么要 1 秒**：

- poll 短 → CPU 高但响应快
- poll 长 → 响应慢但省 CPU
- 1 秒是常用折衷

### 7.1.3 信号处理

```php
private function installSignalHandlers(): void
{
    if (! function_exists('pcntl_signal')) {
        return;
    }
    pcntl_async_signals(true);
    $handler = function (): void {
        $this->shouldQuit = true;
    };
    pcntl_signal(SIGTERM, $handler);
    pcntl_signal(SIGINT, $handler);
}
```

**关键设计**：

- `function_exists('pcntl_signal')` 兜底——Windows 没有 pcntl
- `pcntl_async_signals(true)` —— 异步信号处理，否则 Ctrl+C 要等 `poll()` 结束才响应
- 只监听 `SIGTERM`（k8s 优雅停机）和 `SIGINT`（Ctrl+C），不监听 `SIGQUIT`（强制退出）

**`pcntl_async_signals` 的副作用**：

- 信号到达时立即跳到 handler 执行
- 任何被打断的 PHP 操作都"瞬间完成"
- 对 `$this->shouldQuit = true` 这种简单赋值是安全的

### 7.1.4 优雅退出

```php
$this->info('[kafka:work] shutting down...');
$consumer->close();
$producerFactory->flushAll(5000);
```

**关闭顺序**：

1. `$consumer->close()` —— 释放 partition
2. `$producerFactory->flushAll(5000)` —— 排空所有 in-flight 消息

**为什么必须 flushAll**：

- 在循环里 produce 的消息可能还在 librdkafka 内部 buffer
- 直接 `exit` 会丢消息
- `flush(5000)` 阻塞等 5 秒，强制把 buffer 推给 broker

### 7.1.5 `processMessage()` 三态输出

```php
private function processMessage(HandlerResolver $resolver, Consumer $consumer, Message $message): void
{
    $topic = $message->header('x-original-topic') ?? 'laravel-jobs';

    $handler = $resolver->resolve($topic, $message);
    $result = $handler->handle($message);

    switch ($result->action()) {
        case HandlerResult::ACTION_ACK:
            $this->line(sprintf(
                '<info>ACK</info> offset=%s topic=%s',
                $message->header('x-original-offset') ?? '?',
                $topic
            ));
            break;

        case HandlerResult::ACTION_REQUEUE:
            $this->warn(sprintf(
                'REQUEUE offset=%s topic=%s attempt=%s',
                $message->header('x-original-offset') ?? '?',
                $topic,
                $message->header('x-attempt') ?? '?'
            ));
            break;

        case HandlerResult::ACTION_DLQ:
            $error = $result->error();
            $this->error(sprintf(
                'DLQ offset=%s topic=%s err=%s',
                $message->header('x-original-offset') ?? '?',
                $topic,
                $error !== null ? get_class($error) : 'unknown'
            ));
            break;
    }
}
```

**关键**：三种 action 的实际处理由 Handler + FailedHandler 内部完成，Command 只负责可视化。**不**重复造轮子。

### 7.1.6 `parseTopics()`

```php
private function parseTopics(array $queueOption, string $defaultTopic): array
{
    $topics = [];
    foreach ($queueOption as $opt) {
        foreach (explode(',', (string) $opt) as $t) {
            $t = trim($t);
            if ($t !== '') {
                $topics[] = $t;
            }
        }
    }
    if (count($topics) === 0) {
        $topics[] = $defaultTopic;
    }
    return array_values(array_unique($topics));
}
```

**支持两种风格**：

- `--queue=emails --queue=reports` —— 多个 `--queue` 选项
- `--queue=emails,reports` —— 逗号分隔

**为何 trim + skip empty**：

- 防止用户写 `--queue=,emails,` 这种带逗号边缘
- `array_unique` 去重

## 7.2 ServiceProvider 修补（Step 7 期间）

### 7.2.1 `syncFailedTableConfig()` 桥接

**问题**（开发日志 §16.6 Step 6 已知待办）：

- `DatabaseFailedJobHandler` 写表时读 `kafka.connections.default.failed.database.table`
- Laravel `queue:failed` 命令读 `config('queue.failed.table')`
- 两者不一致时，用户的 `queue:failed` 永远看不到本包写的失败

**修复**：

```php
private function syncFailedTableConfig(): void
{
    $driver = (string) config('kafka.connections.default.failed.driver', 'hybrid');
    if ($driver === 'dlq') {
        return;
    }
    $table = (string) config('kafka.connections.default.failed.database.table', 'failed_jobs');
    config(['queue.failed' => array_merge(
        (array) config('queue.failed', []),
        ['driver' => 'database-uuids', 'database' => config('kafka.connections.default.failed.database.connection'), 'table' => $table]
    )]);
}
```

**做的事**：

1. 读 `kafka.*.failed.database.table`
2. 把它写进 `config('queue.failed')` 数组
3. Laravel 内部 `queue:failed` 命令自动用新值

**`driver: 'database-uuids'`**：

- Laravel 8+ 的 driver 标识
- 对应 `Illuminate\Queue\Failed\DatabaseUuidFailedJobProvider`
- 表结构与我们的 `DatabaseFailedJobHandler` 写入的兼容

### 7.2.2 `registerFailer` 的 `Uuid` 注入修补

**原 Step 2 漏掉的**：

```php
$this->app->singleton('queue.failer.kafka', function ($app) {
    $config = (array) config('kafka.connections.default.failed.database', []);
    $table = (string) ($config['table'] ?? 'failed_jobs');
    $connection = $config['connection'] ?? null;

    $database = $app->make('db')->connection($connection);

    return new DatabaseFailedJobHandler($database, $table);  // ❌ 漏了 Uuid
});
```

**Step 7 修补**：

```php
return new DatabaseFailedJobHandler(
    $database,
    $table,
    $app->make(\Ramsey\Uuid\UuidInterface::class)  // ✅ 注入 Uuid
);
```

**为什么 `$app->make(UuidInterface::class)`**：

- `ramsey/uuid` 包的服务提供者会自动注册 `UuidInterface` 单例
- 直接从容器拿，保证整个项目用同一个 Uuid 工厂
- 避免多个 Uuid 实例导致生成策略不一致

## 7.3 Step 7 小结

| 类 / 方法 | 职责 | 关键设计 |
| --- | --- | --- |
| `WorkCommand` | kafka:work 长驻进程 | 信号处理 + 1s poll + 优雅退出 |
| `syncFailedTableConfig` | queue.failed.table 桥接 | 让 Laravel `queue:failed` 命令与本包共用表 |
| `registerFailer` 修补 | Uuid 注入 | 从容器拿 UuidInterface 单例 |

**关键认知**：

1. **SIGTERM / SIGINT 优雅退出**：`pcntl_async_signals` 让 Ctrl+C 立即响应
2. **退出前 flushAll**：`producer.flush()` 防止 librdkafka 内部 buffer 丢消息
3. **ServiceProvider 桥接**：`syncFailedTableConfig` 是必需的——没有它，用户的 `queue:failed` 永远是空的

---

# Step 8 精读：Support 与 Exceptions

**目标**：辅助类（常量 / 工具 / 异常）的设计。

6 个文件：`Header` / `TraceContext` / `Str` / 3 个异常类

## 8.1 `src/Support/Header.php` —— Kafka Header 常量集中地

**作用**：把 22 个 Kafka Header key 字符串集中定义。

```php
final class Header
{
    public const TRACE_ID = 'x-trace-id';
    public const QUEUE = 'x-queue';
    public const CONNECTION = 'x-connection';
    public const ENQUEUED_AT = 'x-enqueued-at';
    public const RETRY_COUNT = 'x-attempt';
    public const SERIALIZER = 'x-serializer';

    public const AVAILABLE_AT = 'x-available-at';
    public const JOB_ID = 'x-job-id';

    public const ORIGINAL_TOPIC = 'x-original-topic';
    public const ORIGINAL_PARTITION = 'x-original-partition';
    public const ORIGINAL_OFFSET = 'x-original-offset';
    public const ORIGINAL_HEADERS = 'x-original-headers';

    public const FAILED_AT = 'x-failed-at';
    public const EXCEPTION_CLASS = 'x-exception-class';
    public const EXCEPTION_MESSAGE = 'x-exception-message';
    public const EXCEPTION_TRACE = 'x-exception-trace';

    public const HANDLER = 'x-handler';
    public const CONSUMER_GROUP = 'x-consumer-group';

    private function __construct()
    {
    }
}
```

**22 个常量分 6 组**：

| 分组 | 数量 | 用途 |
| --- | --- | --- |
| 基础元信息 | 6 | push 时注入 |
| 延迟 | 1 | available_at |
| Job 标识 | 1 | job_id |
| 原始位置 | 4 | topic/partition/offset/headers |
| DLQ | 4 | failed_at/exception_*/trace |
| 路由（v0.3） | 2 | handler / consumer_group |

**`private function __construct()`**：

- 禁止 `new Header()`
- 类只用作常量容器

**为什么用常量而不是 enum**：

- PHP 7.4 没 enum
- `public const` 已经在 PHP 7.4 支持
- 使用方式：`Header::TRACE_ID` —— 跟 enum 用法几乎一样

**为什么用 `x-` 前缀**：

- 业界惯例：自定义 header 加 `x-` 前缀
- 与 HTTP header `X-Forwarded-For` 等一致
- 避免与 Kafka 自带 / 第三方 header 冲突

## 8.2 `src/Support/TraceContext.php` —— trace_id 工厂

**作用**：生成 trace_id，支持父子派生。

```php
final class TraceContext
{
    public static function next(): string
    {
        return bin2hex(random_bytes(8));
    }

    public static function child(string $parentTraceId, string $suffix = ''): string
    {
        $combined = $parentTraceId . ($suffix !== '' ? ('.' . $suffix) : '');
        return substr(bin2hex(hash('sha256', $combined, true)), 0, 16);
    }
}
```

**`next()`**：

- 16 字符十六进制 = 64 位随机数
- 2^64 = 1.8 × 10^19 空间，碰撞概率忽略不计
- 用 `random_bytes(8)` 而不是 `rand(0, PHP_INT_MAX)`——密码学安全

**`child()`**：

- 父 trace_id + suffix 哈希派生
- 同 input 同 output（确定性）
- 不同 suffix 不同 output

**v0.1 简化为基础实现**：

- 没用 W3C Trace Context 标准（`traceparent` 头）
- 没用 OpenTelemetry SDK
- v0.2 评估接入

**为什么 v0.1 没引用**：

- 整个项目只有 `KafkaQueue::buildMessage` 一处用 `bin2hex(random_bytes(8))`
- `TraceContext::next()` 是为 v0.2 准备的接口位
- v0.1 直接 inline 写更简单

**已知缺陷**（开发日志 §16.8 待办）：

- `child()` 当前没被任何代码调用
- phpstan level 8 会报"undefined usage"

## 8.3 `src/Support/Str.php` —— 字符串工具

**作用**：本包需要但 Laravel `Str` 不直接提供的工具。

```php
final class Str
{
    public static function truncate(string $value, int $maxBytes, string $ellipsis = '...'): string
    {
        if (strlen($value) <= $maxBytes) {
            return $value;
        }
        $cutAt = max(0, $maxBytes - strlen($ellipsis));
        return substr($value, 0, $cutAt) . $ellipsis;
    }

    public static function mask(string $value, int $visibleStart = 2, int $visibleEnd = 2, string $mask = '*'): string
    {
        $len = strlen($value);
        if ($len <= $visibleStart + $visibleEnd) {
            return str_repeat($mask, $len);
        }
        $head = substr($value, 0, $visibleStart);
        $tail = substr($value, -$visibleEnd);
        $middle = str_repeat($mask, $len - $visibleStart - $visibleEnd);
        return $head . $middle . $tail;
    }

    public static function isUuid(string $value): bool
    {
        return (bool) preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i', $value);
    }
}
```

**3 个方法的设计动机**：

### 8.3.1 `truncate()` 字节安全

- 用 `strlen` + `substr` 而不是 `mb_strlen` + `mb_substr`
- 字节级截断，对 UTF-8 可能产生半个字
- **但** Kafka header 是字节流——按字节截断符合协议层

### 8.3.2 `mask()` 敏感信息脱敏

- 例：`KafkaException` log 时把 SASL password mask 掉
- 头 2 字符 + 尾 2 字符可见，中间全 `*`
- 短字符串全部 mask

### 8.3.3 `isUuid()` 校验

- 8-4-4-4-12 格式的 UUID 校验
- 用正则，不用 `Ramsey\Uuid::isValid` —— 减少依赖
- 业务侧判断 Job UUID 是否合法

**为什么不重复 Laravel `Str`**：

- Laravel `Str::limit` 按字符截断，不适合字节流
- Laravel `Str::random` 生成长度固定的字符串
- 本包的 `Str::truncate` 是字节级，**和** Laravel 工具互补

## 8.4 三种异常类

### 8.4.1 `src/Exceptions/KafkaException.php`

```php
class KafkaException extends RuntimeException
{
}
```

**所有 librdkafka 调用失败的统一出口**：

- `KafkaQueue` send / poll / flush 失败
- `Producer::send` timeout
- `Consumer::poll` 错误

**为什么 `extends RuntimeException` 而不是 `extends Exception`**：

- `RuntimeException` 表示"运行时可能发生且可恢复的错误"
- `Exception` 是基类，不区分可恢复性
- Laravel 自己大量用 `RuntimeException` 做业务异常

### 8.4.2 `src/Exceptions/SerializationException.php`

```php
class SerializationException extends RuntimeException
{
}
```

**由 `Serializer` 实现抛出**：

- `PhpSerializer::encode/decode` 失败
- `JsonSerializer::encode/decode` 失败

**特殊地位**：

- 默认加入 `failed.hybrid.fatal_exceptions` 列表
- 序列化失败重试无意义，直接 DLQ

### 8.4.3 `src/Exceptions/DlqException.php`

```php
class DlqException extends RuntimeException
{
}
```

**DLQ 写入失败**：

- v0.1 实际上 `DlqFailedJobHandler` 不抛这个异常（直接传给 Producer.send 让 `KafkaException` 抛）
- 留这个异常类是为 v0.2+ 让 `DlqFailedJobHandler` 显式标注失败类型

## 8.5 Step 8 小结

| 类 | 职责 | 关键设计 |
| --- | --- | --- |
| `Header` | 22 个 Kafka header 常量 | 6 分组 + 禁止实例化 |
| `TraceContext` | trace_id 工厂 | 16 字符随机 + 父子派生 |
| `Str` | 字节安全 truncate / mask / isUuid | 不重复 Laravel `Str` |
| `KafkaException` | Kafka 操作统一异常 | 继承 RuntimeException |
| `SerializationException` | 序列化失败 | 默认 fatal |
| `DlqException` | DLQ 写入失败 | 留 v0.2 增强 |

**关键认知**：

1. **Header 常量集中**避免散落字符串
2. **三个异常类**各自独立，方便业务 catch 特定类型
3. **Str 工具克制**——只做 Laravel `Str` 不直接做的

---

# Step 9 精读：Facades

**目标**：让用户能 `use Kafka;` 用 Facade 调用本包。

1 个文件：`src/Facades/Kafka.php`

## 9.1 `src/Facades/Kafka.php` —— 薄包装

**作用**：把 `KafkaManager` 暴露为全局可调用的 Facade。

```php
final class Kafka extends Facade
{
    protected static function getFacadeAccessor()
    {
        return KafkaManager::class;
    }
}
```

**为什么 `return KafkaManager::class` 而不是 `return 'kafka.manager'`**：

- `KafkaManager::class` 是类名常量，IDE 能跳转
- `'kafka.manager'` 是字符串，IDE 找不到定义
- 两个都能解析（ServiceProvider 已注册 alias），但类名更安全

**`@method static` 注解**：

```php
/**
 * @method static \Illuminate\Contracts\Queue\Queue connection(string $name = null)
 * @method static \LaravelKafka\Config\KafkaConfig config(string $name = null)
 * @method static void disconnect(string $name = null)
 *
 * @see \LaravelKafka\Manager\KafkaManager
 */
```

**给 IDE 看的元数据**：

- IDE 读 `@method` 给出自动补全
- `@see` 告诉 IDE 跳转到 `KafkaManager`
- 业务方 `Kafka::connection('reports')` IDE 直接补全

**v0.1 故意只暴露 3 个方法**：

- `connection($name)` —— 拿 Queue 实例
- `config($name)` —— 拿 KafkaConfig 实例
- `disconnect($name)` —— 释放连接

**不暴露**：

- `send / transaction / replay / dlq` —— v0.2+ 才有的方法
- v0.1 提前暴露 = 挖坑

**为什么 v0.1 还要做 Facade**：

- 用户的 `composer require` 安装后，`use Kafka;` 能直接用
- 心理上"这个包真像 Laravel 一等公民"——生态感
- 实际值不大，但**门面**很重要

## 9.2 Step 9 小结

**核心认知**：

- `KafkaManager::class` 优于 `'kafka.manager'` 字符串
- v0.1 故意**少**暴露方法，避免 API 漂移
- `@method static` 注解是 IDE 体验的关键

---

# Step 10 精读：默认配置

**目标**：让 `vendor:publish` 后用户能直接用的合理默认值。

1 个文件：`config/kafka.php`

## 10.1 `config/kafka.php` 整体结构

```php
return [

    'default' => env('KAFKA_CONNECTION', 'default'),

    'connections' => [

        'default' => [

            'brokers'   => env('KAFKA_BROKERS', 'localhost:9092'),
            'client_id' => env('KAFKA_CLIENT_ID', 'laravel-kafka'),
            'protocol'  => env('KAFKA_PROTOCOL', 'PLAINTEXT'),

            'sasl' => [...],
            'ssl'  => [...],

            'queue'  => env('KAFKA_DEFAULT_TOPIC', 'laravel-jobs'),
            'topics' => [...],

            'producer' => [...],
            'consumer' => [...],
            'failed'   => [...],
            'delay'    => [...],
            'replay'   => [...],

        ],

    ],

];
```

**两级结构**：

- 顶层 `default` —— 当前激活的 connection 名
- 顶层 `connections` —— 所有 connection 配置
- 每个 connection 内部完整独立

## 10.2 关键配置项逐项解析

### 10.2.1 broker 基础

```php
'brokers'   => env('KAFKA_BROKERS', 'localhost:9092'),
'client_id' => env('KAFKA_CLIENT_ID', 'laravel-kafka'),
'protocol'  => env('KAFKA_PROTOCOL', 'PLAINTEXT'),
```

- `brokers` 默认 `localhost:9092`——本地开发友好
- `client_id` 给 broker 看是哪个应用连的，方便运维定位
- `protocol` 4 种之一（PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL）

### 10.2.2 producer 调优

```php
'producer' => [
    'compression'        => env('KAFKA_PRODUCER_COMPRESSION', 'snappy'),
    'batch_size'         => (int) env('KAFKA_PRODUCER_BATCH_SIZE', 10000),
    'linger_ms'          => (int) env('KAFKA_PRODUCER_LINGER_MS', 5),
    'request_timeout_ms' => (int) env('KAFKA_PRODUCER_REQUEST_TIMEOUT_MS', 30000),
    'message_timeout_ms' => (int) env('KAFKA_PRODUCER_MESSAGE_TIMEOUT_MS', 30000),
    'enable_idempotence' => (bool) env('KAFKA_PRODUCER_IDEMPOTENCE', true),
    'acks'               => env('KAFKA_PRODUCER_ACKS', 'all'),
],
```

**7 个字段的设计动机**：

| 字段 | 默认 | 决定动机 |
| --- | --- | --- |
| `compression` | `snappy` | 性能与 CPU 折中（gzip CPU 重，none 不压缩） |
| `batch_size` | 10000 bytes | 单次请求最大体积 |
| `linger_ms` | 5 | 积攒 5ms 等小批量，吞吐↑ 延迟↑ 5ms |
| `request_timeout_ms` | 30000 | 单次 produce 请求超时 |
| `message_timeout_ms` | 30000 | 消息投递总超时（含重试） |
| `enable_idempotence` | `true` | 单分区 exactly-once 关键 |
| `acks` | `all` | leader + 全部 ISR 确认才返回 |

**`acks=all` + `enable_idempotence=true` 的组合**：

- `acks=all` —— 副本都收到才 ack
- `enable_idempotence=true` —— producer 端去重，防止重试导致重复
- 两者结合 → **单分区 exactly-once**（v0.3 加事务变跨分区 exactly-once）

### 10.2.3 consumer 调优

```php
'consumer' => [
    'group_id'              => env('KAFKA_GROUP_ID', 'laravel-default'),
    'auto_offset_reset'     => env('KAFKA_AUTO_OFFSET_RESET', 'error'),
    'enable_auto_commit'    => false,
    'max_poll_interval_ms'  => (int) env('KAFKA_MAX_POLL_INTERVAL_MS', 300000),
    'session_timeout_ms'    => (int) env('KAFKA_SESSION_TIMEOUT_MS', 45000),
    'heartbeat_interval_ms' => (int) env('KAFKA_HEARTBEAT_INTERVAL_MS', 3000),
    'fetch_min_bytes'       => (int) env('KAFKA_FETCH_MIN_BYTES', 1),
    'fetch_max_bytes'       => (int) env('KAFKA_FETCH_MAX_BYTES', 52428800),
    'isolation_level'       => env('KAFKA_ISOLATION_LEVEL', 'read_committed'),
],
```

**9 个字段的设计动机**：

| 字段 | 默认 | 决定动机 |
| --- | --- | --- |
| `group_id` | `laravel-default` | 消费者组 ID，决定 offset 提交到哪 |
| `auto_offset_reset` | `error` | 没 committed offset 时**报错**，避免意外从头消费 |
| `enable_auto_commit` | `false` | 必须手动 commit（at-least-once 语义） |
| `max_poll_interval_ms` | 300000（5 分钟） | 单次 poll 间隔上限，超出触发 rebalance |
| `session_timeout_ms` | 45000 | 消费者心跳超时 |
| `heartbeat_interval_ms` | 3000 | 心跳间隔（应小于 session_timeout / 3） |
| `fetch_min_bytes` | 1 | broker 至少攒 1 字节才返回（可调高） |
| `fetch_max_bytes` | 50MB | 单次 fetch 最大体积 |
| `isolation_level` | `read_committed` | 只读已提交的事务消息（配合 producer 事务） |

**`auto_offset_reset = error` 的关键**：

- librdkafka 三种行为：`earliest`（从头）/ `latest`（从尾）/ `error`（报错）
- 默认选 `error` 是因为：
  - `earliest` 可能让新 group 误消费历史数据
  - `latest` 可能让新 group 丢数据
  - `error` 强制用户显式决定——避免沉默错误

### 10.2.4 failed 三模式

```php
'failed' => [
    'driver'   => env('KAFKA_FAILED_DRIVER', 'hybrid'),

    'database' => [
        'table'      => env('KAFKA_FAILED_TABLE', 'failed_jobs'),
        'connection' => env('KAFKA_FAILED_DB_CONNECTION'),
    ],

    'dlq' => [
        'topic'             => env('KAFKA_DLQ_TOPIC'),
        'auto_topic_suffix' => env('KAFKA_DLQ_AUTO_SUFFIX', '.dlq'),
        'retention_ms'      => (int) env('KAFKA_DLQ_RETENTION_MS', 1209600000),  // 14d
    ],

    'hybrid' => [
        'fatal_exceptions' => [...],
        'max_attempts'           => (int) env('KAFKA_MAX_ATTEMPTS', 3),
        'trace_truncate_bytes'   => (int) env('KAFKA_TRACE_TRUNCATE', 32768),
        'message_truncate_bytes' => (int) env('KAFKA_MESSAGE_TRUNCATE', 4096),
    ],
],
```

**与 §3.6 设计一一对应**：

- `driver` 默认 `hybrid`（用户决议）
- `database.table` 默认 `failed_jobs`（Laravel 兼容）
- `dlq.auto_topic_suffix` 默认 `.dlq`（DLQ topic 自动拼接）
- `dlq.retention_ms` 默认 14 天
- `hybrid.max_attempts` 默认 3 次
- `hybrid.fatal_exceptions` 默认空数组（用户在生产配置里加）

## 10.3 Step 10 小结

**核心认知**：

1. **环境变量 + 默认值**双层：开发友好（用默认值能跑），生产可覆盖
2. **`auto_offset_reset = error`** 是重要的安全设计：避免新 group 误消费历史
3. **`enable_idempotence = true` + `acks = all`** 是 exactly-once 基础
4. **`failed.driver = hybrid`** 是用户决议的默认值

---

# Step 11 精读：测试骨架

**目标**：建立可被 CI 跑的测试基线。

11 个文件：phpunit.xml + TestCase + 9 个单元测试

## 11.1 `phpunit.xml` —— PHPUnit 配置

```xml
<phpunit
    bootstrap="vendor/autoload.php"
    backupGlobals="false"
    beStrictAboutTestsThatDoNotTestAnything="false"
    colors="true"
    processIsolation="false"
    stopOnFailure="false"
    cacheResultFile=".phpunit.cache/test-results"
    executionOrder="random"
    resolveDependencies="true"
>
```

**关键配置**：

- `bootstrap="vendor/autoload.php"` —— 从 vendor 加载 PSR-4
- `backupGlobals="false"` —— 不备份 `$GLOBALS`（性能）
- `executionOrder="random"` —— 随机执行测试，避免依赖顺序
- `resolveDependencies="true"` —— 自动解析 `@depends` 注解
- `cacheResultFile` —— 缓存测试结果，加速 PHPUnit 自身

```xml
<testsuites>
    <testsuite name="Unit">
        <directory>tests/Unit</directory>
    </testsuite>
    <testsuite name="Feature">
        <directory>tests/Feature</directory>
    </testsuite>
    <testsuite name="Integration">
        <directory>tests/Integration</directory>
    </testsuite>
</testsuites>
```

**3 个 test suite 划分**：

- `Unit` —— 纯逻辑，无外部依赖（CI 必跑）
- `Feature` —— 业务流程，mock 外部依赖（CI 跑）
- `Integration` —— 真实 Kafka 集群，v0.1 跳过

```xml
<php>
    <env name="APP_ENV" value="testing"/>
    <env name="KAFKA_BROKERS" value="localhost:9092"/>
    <env name="KAFKA_DEFAULT_TOPIC" value="laravel-jobs-test"/>
    <env name="KAFKA_FAILED_DRIVER" value="hybrid"/>
</php>
```

**测试环境变量**：

- `APP_ENV=testing` —— Laravel 测试环境标识
- 独立的测试 topic（`laravel-jobs-test`）避免污染开发数据
- 失败模式用 hybrid 测全路径

## 11.2 `tests/TestCase.php` —— Testbench 抽象基类

**作用**：让每个测试都能启动完整 Laravel 容器，注册本包 ServiceProvider。

```php
abstract class TestCase extends Orchestra
{
    protected function getPackageProviders($app): array
    {
        return [
            LaravelKafkaServiceProvider::class,
        ];
    }

    protected function getPackageAliases($app): array
    {
        return [
            'Kafka' => \LaravelKafka\Facades\Kafka::class,
        ];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('kafka.connections.default', [
            'brokers' => 'localhost:9092',
            'client_id' => 'laravel-kafka-test',
            'protocol' => 'PLAINTEXT',
            'queue' => 'laravel-jobs-test',
            'topics' => [],
            'producer' => [],
            'consumer' => ['group_id' => 'laravel-test'],
            'failed' => [
                'driver' => 'hybrid',
                'database' => ['table' => 'failed_jobs_test'],
                'dlq' => ['topic' => 'laravel-jobs-test.dlq'],
                'hybrid' => ['max_attempts' => 3],
            ],
            'delay' => [],
            'replay' => [],
        ]);
    }
}
```

**Orchestra Testbench 抽象**：

- `getPackageProviders` —— 注册 ServiceProvider
- `getPackageAliases` —— 注册 Facade 别名
- `defineEnvironment` —— 每个测试前重设 config（保证隔离）

**为什么每个测试要重设 config**：

- 测试间可能修改 config（虽然 v0.1 不多）
- 防止上一个测试的副作用影响下一个
- 让每个测试**独立**可重复

**为什么不放 fixtures 在 `setUp`**：

- `defineEnvironment` 在 Laravel 启动**前**执行
- 写进 `setUp` 会被 `setUp` 里访问 config 的代码"读不到"

## 11.3 9 个单元测试精读

### 11.3.1 `HeaderTest.php`

**测什么**：

- 22 个常量值正确
- 类是 `final`
- 构造器是 `private`（不能 `new`）

**为什么测常量**：

- 防止重构时改错（`Header::TRACE_ID` 从 `'x-trace-id'` 改成 `'trace-id'` 会破兼容）
- 把"硬编码字符串"在测试里再写一遍——冗余但必要

### 11.3.2 `StrTest.php`

**测什么**：

- `truncate` 三种情况：短于 max / 长于 max / 恰好等于 max
- `mask` 基础 + 短字符串
- `isUuid` 合法 + 非法

**为什么测 `strlen <= $maxBytes`**：

- 边界值：恰好等于 max 时不应该加 `...` 省略号
- 短字符串不能截断后还加 `...`

### 11.3.3 `TraceContextTest.php`

**测什么**：

- `next()` 长度 16
- 100 次调用结果唯一
- `child()` 确定性（同 input 同 output）
- `child()` 不同 suffix 输出不同

**为什么测 100 次唯一**：

- 简单概率验证——`random_bytes(8)` 应该不会重复
- 实际生产 1.8 × 10^19 空间远大于 100

### 11.3.4 `MessageTest.php`

**测什么**：

- 5 个字段访问器
- `header($name, $default)` 默认值
- `withHeaders` 不可变
- `withKey` 不可变
- `withHeader` 单字段

**为什么重点测不可变**：

- `Message` 是核心值对象——任何人改了不可变语义，整个系统会崩
- `withHeaders` 不能修改原对象，只返回新对象

### 11.3.5 `PhpSerializerTest.php`

**测什么**：

- 标量 / 数组 / 对象 / 空字符串 / 无效 payload
- `name()` 返回 'php'

**为什么测 `unserialize` 失败**：

- `unserialize('not-valid')` 返回 `false` 而不是抛异常
- 我们包成 `SerializationException`
- 测试验证"返回 false → 抛异常"的逻辑

### 11.3.6 `JsonSerializerTest.php`

**测什么**：

- Unicode 不被转义（`JSON_UNESCAPED_UNICODE`）
- 斜杠不被转义（`JSON_UNESCAPED_SLASHES`）
- 空字符串
- 无效 JSON
- `name()` 返回 'json'

**为什么测 Unicode / 斜杠**：

- 这两个 flag 是 v0.1 设计决策
- 万一 librdkafka 升级 / PHP 升级改变 JSON 行为，测试能立刻发现

### 11.3.7 `ExceptionTest.php`

**测什么**：

- 三个异常都继承 `RuntimeException`
- 三个异常都实现 `Throwable`
- message 保留
- 链式异常

**为什么测 extends RuntimeException**：

- 业务层 catch `RuntimeException` 应该能 catch 本包异常
- 万一后续重构改了基类，业务层逻辑会断

### 11.3.8 `KafkaConfigTest.php`

**测什么**：

- `fromArray` 默认值
- 异常路径：empty brokers / empty topic / invalid protocol
- `resolveTopic` 三种 fallback
- `toProducerRdKafkaConfig` 翻译
- `toConsumerRdKafkaConfig` 翻译
- SASL 字段注入

**为什么测最详细**：

- `KafkaConfig` 是"单一真相源"
- 翻译逻辑错一个字母，整个 Kafka 集群通信挂
- 10 个测试覆盖核心场景

## 11.4 Step 11 小结

**核心认知**：

1. **phpunit.xml 三个 test suite**：Unit 必跑、Feature 跑、Integration 跳过
2. **`defineEnvironment` 重设 config**：测试隔离关键
3. **22 个 Header 常量测**：防重构误改
4. **`KafkaConfigTest` 是测试最厚**的：因为是翻译层，错了影响所有下游

---

# Step 12 精读：CI 配置

**目标**：让 GitHub Actions 跑通测试 + linter。

5 个文件：2 个 workflow + CODEOWNERS + 2 个模板

## 12.1 `.github/workflows/tests.yml` —— 测试矩阵

**核心是矩阵**：

```yaml
matrix:
  include:
    - php: '7.4'
      laravel: '8.*'
      testbench: '6.*'
      composer: '2.2'
    - php: '8.1'
      laravel: '9.*'
      testbench: '7.*'
      composer: '2.5'
    - php: '8.1'
      laravel: '10.*'
      testbench: '8.*'
      composer: '2.5'
    - php: '8.1'
      laravel: '11.*'
      testbench: '9.*'
      composer: '2.5'
    - php: '8.3'
      laravel: '10.*'
      testbench: '8.*'
      composer: '2.7'
    - php: '8.3'
      laravel: '11.*'
      testbench: '9.*'
      composer: '2.7'
```

**6 个组合**：

- 7.4 + Laravel 8（PHP 7.4 唯一能跑的 Laravel）
- 8.1 + Laravel 9/10/11（3 组合，PHP 8.1 主流）
- 8.3 + Laravel 10/11（2 组合，PHP 8.3 最新）

**Testbench 版本与 Laravel 严格对应**（来自开发日志 §16.12 收尾）：

- Laravel 8 → Testbench 6
- Laravel 9 → Testbench 7
- Laravel 10 → Testbench 8
- Laravel 11 → Testbench 9

**为什么在 matrix 里直接铺**（`matrix.testbench` 字段）：

- GH Actions `${{ }}` 表达式**不**支持 `&&` `||` 逻辑运算
- 在 matrix 里直接铺好对应关系是最稳妥的写法

### 12.1.1 步骤详解

```yaml
- name: Install librdkafka
  run: |
    sudo apt-get update
    sudo apt-get install -y librdkafka-dev

- name: Setup PHP
  uses: shivammathur/setup-php@v2
  with:
    php-version: ${{ matrix.php }}
    extensions: rdkafka
    tools: composer:${{ matrix.composer }}
    coverage: none
```

**`shivammathur/setup-php@v2` + `extensions: rdkafka`**：

- 一键装 PHP + ext-rdkafka + librdkafka
- 不用手动 `apt-get install librdkafka-dev + pecl install rdkafka`

```yaml
- name: Install dependencies
  run: |
    composer require \
      "illuminate/queue:${{ matrix.laravel }}" \
      "illuminate/support:${{ matrix.laravel }}" \
      "illuminate/console:${{ matrix.laravel }}" \
      "illuminate/contracts:${{ matrix.laravel }}" \
      "orchestra/testbench:${{ matrix.testbench }}" \
      --no-update --no-interaction
    composer update --prefer-dist --no-interaction
```

**`--no-update` + `composer update`**：

- 先 `require --no-update` 把版本号写进 composer.json
- 再 `composer update` 一次性解析依赖

**为什么不写死 `composer.json`**：

- composer.json 里写 `^8 || ^9 || ^10 || ^11` 让 CI 自动选
- CI 用 `composer require` 动态指定 `8.*` 强制

### 12.1.2 集成测试 disabled

```yaml
integration:
  name: Integration Tests (Testcontainers)
  runs-on: ubuntu-latest
  if: false  # ← 关键
  steps:
    - name: Start Kafka via Testcontainers
      run: vendor/bin/testcontainers start kafka:3.6.1
```

**`if: false`**：

- Job 永远不跑
- 留位置等 v0.2 接 Testcontainers 时打开

## 12.2 `.github/workflows/linter.yml` —— 独立 linter

**为什么独立**：

- 与 tests 并行跑，加快反馈
- linter 不需要矩阵（只 PHP 8.1 跑一次）
- linter 失败不阻塞 tests

**2 个 job**：

- `php-cs-fixer` —— 风格检查
- `phpstan` —— 静态分析

```yaml
- name: Check style
  run: vendor/bin/php-cs-fixer fix --dry-run --diff
```

`--dry-run` 不改文件，只看会改什么。CI 失败时给出 diff。

## 12.3 `.github/CODEOWNERS` —— 私有期 owner

```text
*       @Lyn-Huang
/docs/  @Lyn-Huang
/RFC/   @Lyn-Huang
```

**私有期只有 1 个 owner**：

- 所有 PR 自动请求 owner 审核
- `/docs/` 和 `/RFC/` 显式列出（避免 markdown 被机器人自动改）

## 12.4 `.github/ISSUE_TEMPLATE/bug_report.md` & `PULL_REQUEST_TEMPLATE.md`

**Bug 报告 6 个必填项**：

1. 描述
2. 复现步骤
3. 预期 / 实际
4. 环境（PHP / Laravel / 扩展版本 / Kafka / ext-rdkafka / librdkafka）
5. 附加上下文

**为什么必填 librdkafka + ext-rdkafka 版本**：

- `php -r "echo RD_KAFKA_VERSION;"` —— ext-rdkafka 版本
- `php -r "echo rd_kafka_version_str();"` —— librdkafka 版本
- 这两个版本不匹配是 Kafka 客户端问题最常见原因

**PR 模板 7 项检查清单**：

- composer validate
- phpunit Unit
- php-cs-fixer
- phpstan
- 新类有单元测试
- 文档同步
- CHANGELOG 更新

## 12.5 Step 12 小结

**核心认知**：

1. **测试矩阵 6 组合**覆盖 PHP 7.4 / 8.1 / 8.3 × Laravel 8/9/10/11
2. **`shivammathur/setup-php@v2`** 一键装环境
3. **`if: false` 跳过 Integration** 等 v0.2 Testcontainers
4. **linter 独立 job** 与 tests 并行

---

# Step 13 精读：RFC 归档

**目标**：把决策以 RFC 格式归档，追溯"为什么这么定"。

2 个文件：RFC/0001-initial.md / RFC/0002-meta.md

## 13.1 RFC 标准结构

**7 段式**：

1. 状态（Accepted / Pending / Superseded）
2. 日期
3. 摘要（关键决策表格）
4. 详细方案（指回设计文档）
5. 兼容性影响
6. 替代方案（被否决的 + 否决理由）
7. 决策（一句话确认）

## 13.2 RFC 0001 - 初始设计

**4 项决策**：

- 包名 `lyn-huang/laravel-kafka`
- PHP 7.4+
- ext-rdkafka 强制
- 失败模式 database / dlq / hybrid

**为什么 RFC 比开发文档重要**：

- 开发文档说"现在怎么设计"
- RFC 说"当时为什么选 A 不选 B"
- 未来 Reviewer 想知道决策动机，RFC 是唯一来源

## 13.3 RFC 0002 - 项目元信息

**6 项决策**：

- Horizon / Octane / UI 推迟 v0.4
- License MIT
- CI 矩阵 3 个 PHP 版本
- 仓库先私有

**RFC 0002 的特殊价值**：

- 把"延后事项"显式记录——避免被 PR 反复问"要不要顺手做 Horizon"

## 13.4 Step 13 小结

**核心认知**：

1. **RFC ≠ 开发文档**：决策动机的追溯
2. **替代方案段是 RFC 核心**：未来需要"为什么选 X"
3. **延后事项显式记录**：避免 PR 反复提

---

# Step 14 精读：CHANGELOG

**目标**：给用户看"v0.1.0 改了什么"。

1 个文件：docs/CHANGELOG.md

## 14.1 Keep a Changelog 格式

```markdown
## [0.1.0] - 2026-08-20

### Added
#### 基础功能
- ...
#### 失败处理
- ...
#### Producer 子系统
- ...

### 决议
- ...

### Known Issues
- ...
```

**6 大分类**（v0.1 只用 Added）：

- **Added** —— 新增功能
- **Changed** —— 已有功能变更
- **Deprecated** —— 即将删除
- **Removed** —— 已删除
- **Fixed** —— Bug 修复
- **Security** —— 安全修复

## 14.2 按子系统列出

v0.1 把 13 个子系统分类：

1. 基础功能（Connector / Queue / Job / 命令）
2. 失败处理（三模式）
3. Producer 子系统
4. Consumer 子系统
5. 配置与基础设施
6. Support & Exceptions
7. 工具链
8. 文档

**为什么按子系统**：

- Reviewer 快速定位"这块变更在哪"
- 用户能按子系统跳读

## 14.3 决策段独立列出

7 项关键决策汇总：

- 包名 / PHP / rdkafka / 失败模式 / License / 仓库 / Horizon 推迟

**为什么独立段**：

- 决策与代码变更不同性质
- 用户快速回顾"v0.1 是什么"

## 14.4 Known Issues 段

**诚实记录**：

- Windows pcntl 不可用
- Integration CI 跳过
- DLQ truncate 字节截断
- **所有源代码未在本地实际跑过**

**为什么必须诚实**：

- 避免给用户"已通过测试"的错觉
- 失败时用户不会说"你骗我"

## 14.5 Step 14 小结

**核心认知**：

1. **Keep a Changelog 格式**是 PHP 社区标准
2. **按子系统分类**便于 Reviewer 定位
3. **Known Issues 诚实记录**建立信任

---

# 附录 A：常见拓展练习

如果你想真正吃透本教程，建议做以下练习：

### A.1 跑通本地 Kafka

- 按 §3 部署一个 Docker 单 broker
- 跑 §4 所有 CLI 命令
- 尝试 `kafka-console-producer` + `kafka-console-consumer` 联调

### A.2 在 Laravel 项目里集成

- 在一个真实 Laravel 项目里 `composer require` 本包（v0.2 后正式发布）
- 写一个 Job 推到 Kafka
- 跑 `kafka:work` 消费
- 故意抛异常，验证 `failed_jobs` 表 + DLQ topic

### A.3 实现一个 v0.2 功能

- 多 Topic 路由（修改 `KafkaConfig::resolveTopic`）
- Key Routing（修改 `KafkaQueue::push` 接受 `$key` 参数）
- 时间轮延迟（写 `DelayRouter` 类）

### A.4 写第一个 Integration 测试

- 启动 Testcontainers Kafka 容器
- 写一个真实 push + consume 的端到端测试
- 把 CI 的 `if: false` 改成 `if: true`

## 附录 B：进一步阅读

- Apache Kafka 官方文档：<https://kafka.apache.org/documentation/>
- Kafka 入门教程（本文档 `Kafka入门教程.md`）
- 设计文档（本文档 `开发文档_v0.1.md`）
- 实施日志（`docs/开发日志_v0.1.md`）

---

**教程结束**

如果读到这里：v0.1 全部 45 个 PHP 文件 + 5 个配置文件 + 8 个文档，约 100 KB 源码 + 50 KB 文档，已经讲完了。

接下来可以做的：
- 在 Linux/macOS 环境跑 `composer validate` + `phpunit` 验证
- 提交到 GitHub 私有仓库观察 CI
- 开 RFC 0003 启动 v0.2
