<?php

declare(strict_types=1);

namespace LaravelKafka\Queue;

use Illuminate\Queue\Connectors\ConnectorInterface;
use LaravelKafka\Manager\KafkaManager;

/**
 * Laravel Queue 框架识别 Kafka 驱动的入口（v0.1）。
 *
 * ## 角色
 *
 * 在 `config/queue.php` 配 `'driver' => 'kafka'` 时，Laravel 通过
 * `Queue::extend('kafka', ...)` 注册本类。
 * Laravel 调 `connect($config)` 拿一个 `Queue` 实例（即 {@see KafkaQueue}）。
 *
 * ## 业务方使用
 *
 * ```php
 * // config/queue.php
 * 'connections' => [
 *     'kafka' => [
 *         'driver' => 'kafka',
 *         'name'   => 'default',   // 来自 kafka.connections.default
 *     ],
 * ],
 *
 * // 业务代码
 * Queue::push(new MyJob());   // 走 KafkaQueue::push()
 * ```
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 把 connector 逻辑写在 Manager 里（Manager 实现 `ConnectorInterface`），
 * 我们用独立 Connector 类（关注点分离）。
 */
final class KafkaConnector implements ConnectorInterface
{
    private KafkaManager $manager;

    /**
     * @param KafkaManager $manager 单例 KafkaManager（ServiceProvider 注入）
     */
    public function __construct(KafkaManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * Laravel Queue 框架调本方法拿一个 `Queue` 实例。
     *
     * ## 流程
     *
     *  1. 取 `$config['name']`（对应 `kafka.connections.{name}` 的 name，默认 'default'）
     *  2. `$manager->connection($name)` 拿 KafkaQueue（单例缓存）
     *  3. 防御性断言：必须是 KafkaQueue 实例
     *
     * @param array<string,mixed> $config 来自 `config/queue.php` 的连接配置
     * @return KafkaQueue
     * @throws \RuntimeException 防御性断言失败时（异常情况，业务方不会触发）
     */
    public function connect(array $config): KafkaQueue
    {
        $name = (string) ($config['name'] ?? 'default');
        $queue = $this->manager->connection($name);
        if (! $queue instanceof KafkaQueue) {
            // 正常情况不会发生；防御性断言
            throw new \RuntimeException(sprintf(
                'Kafka connection [%s] is not a KafkaQueue instance (got %s).',
                $name,
                is_object($queue) ? get_class($queue) : gettype($queue)
            ));
        }
        return $queue;
    }
}
