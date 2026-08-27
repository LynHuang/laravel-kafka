<?php

declare(strict_types=1);

namespace LaravelKafka\Manager;

use Illuminate\Contracts\Queue\Queue;
use LaravelKafka\Config\KafkaConfig;
use LaravelKafka\Exceptions\KafkaException;

/**
 * 管理多个 Kafka 连接（按 connection name 区分）。
 *
 * ## 用法
 *
 * ```php
 * $manager = app('kafka.manager');
 * $queue = $manager->connection();           // default
 * $queue = $manager->connection('reports');  // named
 * $config = $manager->config();             // default 的 KafkaConfig
 * ```
 *
 * ## 缓存策略
 *
 * - `connections` 数组按 name 缓存 `Queue` 实例（**懒加载**）
 * - `configs` 数组按 name 缓存 `KafkaConfig` 实例
 * - `disconnect()` 释放缓存（Octane 场景用）
 * - 业务方重复调 `connection('x')` 拿到同一个实例
 *
 * ## Fake 模式
 *
 * v0.2 引入：`fake()` 切换到 fake 模式，`isFake()` 查询。
 * fake 模式下 `KafkaQueue::pushRaw` 走 fake 分支，不真发 Kafka。
 *
 * @see \LaravelKafka\Facades\Kafka::fake() 切换到 fake 模式
 * @see \LaravelKafka\Queue\KafkaQueue::pushRaw() 检查 fake 标志
 */
final class KafkaManager
{
    /**
     * 缓存的 Queue 实例数组（按 connection name 索引）。
     *
     * @var array<string, Queue>
     */
    private array $connections = [];

    /**
     * 缓存的 KafkaConfig 实例数组（按 connection name 索引）。
     *
     * @var array<string, KafkaConfig>
     */
    private array $configs = [];

    /**
     * Queue 装配工厂（构造 Producer / Consumer / FailedHandler 并组装 KafkaQueue）。
     *
     * @var ConnectionFactory
     */
    private ConnectionFactory $factory;

    /**
     * Fake 模式开关（v0.2 引入）。
     *
     * - `false`（默认）：`KafkaQueue::pushRaw` 真发 Kafka
     * - `true`：fake 模式，只记录到 FakeMessageStorage，不连 broker
     *
     * 由 {@see \LaravelKafka\Facades\Kafka::fake()} 设为 true，业务方不直接调。
     *
     * @var bool
     */
    private bool $fakeMode = false;

    /**
     * 构造时注入 Queue 装配工厂。
     *
     * @param ConnectionFactory $factory
     */
    public function __construct(ConnectionFactory $factory)
    {
        $this->factory = $factory;
    }

    /**
     * 拿到（或懒加载）指定 connection 的 Queue 实例。
     *
     * 第一次调用会触发：
     *  1. `KafkaConfig::fromArray()` 解析 config
     *  2. `ConnectionFactory::make()` 装配 KafkaQueue
     *
     * 之后调用直接返回缓存实例。
     *
     * @param string|null $name connection 名；null = 用 `kafka.default` 配置的默认值
     * @return Queue
     * @throws KafkaException 指定 connection 名未在 config 中定义时
     */
    public function connection(?string $name = null): Queue
    {
        $name ??= $this->getDefaultConnection();

        if (! isset($this->connections[$name])) {
            $config = $this->resolveConfig($name);
            $this->connections[$name] = $this->factory->make($config, $name);
        }

        return $this->connections[$name];
    }

    /**
     * 拿到指定 connection 的 KafkaConfig 值对象。
     *
     * 不会触发连接 / 装配（纯配置层），比 {@see connection()} 更轻。
     *
     * @param string|null $name connection 名；null = 默认
     * @return KafkaConfig
     * @throws KafkaException 指定 connection 名未在 config 中定义时
     */
    public function config(?string $name = null): KafkaConfig
    {
        $name ??= $this->getDefaultConnection();
        if (! isset($this->configs[$name])) {
            $this->resolveConfig($name);
        }
        return $this->configs[$name];
    }

    /**
     * 断开指定 connection，释放 rdkafka 资源（懒加载缓存）。
     *
     * ## 使用场景
     *
     * - **Laravel Octane** 等长生命周期进程：每请求结束释放 fd
     * - **PHP-FPM 短进程**：通常**不**需要调（进程结束自动释放）
     * - **测试**：同一进程内多次 `Kafka::fake()` 切换
     *
     * 调用后：
     * - `connections[$name]` 被 unset
     * - `configs[$name]` 也被 unset（连带失效）
     * - 下次 `connection($name)` 重新走懒加载路径
     *
     * @param string|null $name connection 名；null = 默认
     * @return void
     */
    public function disconnect(?string $name = null): void
    {
        $name ??= $this->getDefaultConnection();
        unset($this->connections[$name], $this->configs[$name]);
    }

    /**
     * 切换到 fake 模式（v0.2 引入）。
     *
     * fake 模式下，{@see \LaravelKafka\Queue\KafkaQueue::pushRaw()} 不真发 Kafka，
     * 只把消息记录到 {@see \LaravelKafka\Support\Testing\FakeMessageStorage}。
     *
     * **业务方不直接调**——通过 `Kafka::fake()` 触发。
     *
     * 是一次性开关（没有 `unfake()`），要回真发只能 `disconnect()` 重建 manager。
     *
     * @return void
     */
    public function fake(): void
    {
        $this->fakeMode = true;
    }

    /**
     * 查询当前是否处于 fake 模式。
     *
     * 由 {@see \LaravelKafka\Queue\KafkaQueue::pushRaw()} 调用，决定走真发还是 fake 分支。
     *
     * @return bool true = fake 模式，false = 真发模式
     */
    public function isFake(): bool
    {
        return $this->fakeMode;
    }

    /**
     * 把 config 数组喂进 manager（由 ServiceProvider 启动时调用）。
     *
     * 业务方**不直接调**——由 `LaravelKafkaServiceProvider::register()` 调用一次。
     *
     * @param array<string, array<string,mixed>> $connections connection name → config 数组
     * @return void
     */
    public function registerConnections(array $connections): void
    {
        foreach ($connections as $name => $cfg) {
            $this->configs[(string) $name] = KafkaConfig::fromArray((string) $name, $cfg);
        }
    }

    /**
     * 内部：解析 KafkaConfig（不存在则抛异常）。
     *
     * @param string $name connection 名
     * @return KafkaConfig
     * @throws KafkaException 指定 connection 名未在 config 中定义时
     */
    private function resolveConfig(string $name): KafkaConfig
    {
        if (! isset($this->configs[$name])) {
            throw new KafkaException(sprintf('Kafka connection [%s] is not defined.', $name));
        }
        return $this->configs[$name];
    }

    /**
     * 内部：拿到默认 connection 名（从 `kafka.default` config 读）。
     *
     * 三层兜底：
     *  1. 业务方 `config('kafka.default')` 显式配置
     *  2. 没设 → 'default'
     *  3. 设了空字符串 → 'default'
     *
     * @return string
     */
    private function getDefaultConnection(): string
    {
        $name = (string) (function_exists('config') ? config('kafka.default', 'default') : 'default');
        if ($name === '') {
            $name = 'default';
        }
        return $name;
    }
}
