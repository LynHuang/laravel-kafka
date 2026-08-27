<?php

declare(strict_types=1);

namespace LaravelKafka\Manager;

use LaravelKafka\Config\KafkaConfig;
use LaravelKafka\Consumer\ConsumerFactory;
use LaravelKafka\Producer\ProducerFactory;
use LaravelKafka\Queue\Failed\FailedJobHandlerFactory;
use LaravelKafka\Queue\KafkaQueue;

/**
 * KafkaQueue 工厂（v0.1）。
 *
 * ## 角色
 *
 * 根据 {@see KafkaConfig} 构造 {@see KafkaQueue} 实例。
 * 与 {@see \LaravelKafka\Manager\KafkaManager} 配合：
 *  - Manager 负责"哪个连接是 default"（缓存 / 切换）
 *  - Factory 负责"怎么造一个连接"（装配 Producer + Consumer + FailedHandler）
 *
 * ## 装配过程
 *
 * ```
 * KafkaConfig
 *   ├── ProducerFactory::make()    → Producer
 *   ├── ConsumerFactory::make()    → Consumer
 *   └── FailedJobHandlerFactory::make() → FailedJobHandlerInterface
 *                                          │
 *                                          ▼
 *                                      KafkaQueue
 * ```
 *
 * ## 单例复用
 *
 * `Producer` / `Consumer` / `FailedHandler` 各自有 Factory 内部缓存，
 * 多次 `make()` 同一 config 不会重复 `new RdKafka\Producer`。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 把连接构造直接写在 `Manager::connection()` 里；
 * 我们用独立 Factory，Manager 只做缓存 + 路由。
 */
final class ConnectionFactory
{
    private ProducerFactory $producerFactory;

    private ConsumerFactory $consumerFactory;

    private FailedJobHandlerFactory $failedHandlerFactory;

    /**
     * Laravel 容器（注入到 KafkaQueue 让 fake / dispatch 事件能工作）。
     */
    private \Illuminate\Contracts\Container\Container $container;

    /**
     * @param ProducerFactory $producerFactory 已缓存的 producer 工厂
     * @param ConsumerFactory $consumerFactory 已缓存的 consumer 工厂
     * @param FailedJobHandlerFactory $failedHandlerFactory 已缓存的 failed 工厂
     * @param \Illuminate\Contracts\Container\Container $container Laravel 容器
     */
    public function __construct(
        ProducerFactory $producerFactory,
        ConsumerFactory $consumerFactory,
        FailedJobHandlerFactory $failedHandlerFactory,
        \Illuminate\Contracts\Container\Container $container
    ) {
        $this->producerFactory = $producerFactory;
        $this->consumerFactory = $consumerFactory;
        $this->failedHandlerFactory = $failedHandlerFactory;
        $this->container = $container;
    }

    /**
     * 构造 KafkaQueue（不一定单例：Manager 外部缓存 KafkaQueue 实例）。
     *
     * @param KafkaConfig $config
     * @param string $connectionName Laravel connection 名（如 'default' / 'reports'）
     * @return KafkaQueue
     */
    public function make(KafkaConfig $config, string $connectionName): KafkaQueue
    {
        $producer = $this->producerFactory->make($config);
        $consumer = $this->consumerFactory->make($config);
        $failedHandler = $this->failedHandlerFactory->make($config);

        $queue = new KafkaQueue(
            $producer,
            $consumer,
            $failedHandler,
            $config,
            $connectionName,
        );

        // v0.1 老 bug 修复：注入容器（让 KafkaQueue 能 fake / dispatch 事件）
        // 父类 Illuminate\Queue\Queue 的 $container 字段是 protected，
        // v0.4.8 改用具体类 Illuminate\Container\Container (与父类对齐)
        /** @phpstan-ignore-next-line */
        $queue->setContainer($this->container);

        return $queue;
    }
}
