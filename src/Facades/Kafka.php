<?php

declare(strict_types=1);

namespace LaravelKafka\Facades;

use Illuminate\Support\Facades\Facade;
use LaravelKafka\Manager\KafkaManager;
use LaravelKafka\Support\Testing\FakeMessageStorage;
use LaravelKafka\Support\Testing\KafkaFake;

/**
 * Kafka Facade。
 *
 * ## 普通用法
 *
 * ```php
 * use LaravelKafka\Facades\Kafka;
 *
 * $queue = Kafka::connection();          // 拿默认连接的 Queue
 * $queue = Kafka::connection('reports');  // 拿指定 connection 的 Queue
 * $config = Kafka::config();             // 拿默认连接的 KafkaConfig
 * Kafka::disconnect();                   // 释放默认连接
 * ```
 *
 * ## 单元测试用法（v0.2 引入）
 *
 * ```php
 * use LaravelKafka\Facades\Kafka;
 *
 * Kafka::fake();   // 启用 fake 模式
 *
 * Queue::push(new MyJob());   // 不会真发 Kafka, 只记录
 *
 * Kafka::assertPushedOn('orders.created', fn (string $t, Message $m) =>
 *     str_contains($m->payload(), 'amount=100')
 * );
 * Kafka::assertNothingPushed();   // 断言没有消息被发
 * ```
 *
 * ## Facade 解析
 *
 * - `connection / config / disconnect / fake / isFake` 委托给 {@see KafkaManager}
 * - `fake()` 静态方法额外操作容器
 *
 * @method static \Illuminate\Contracts\Queue\Queue connection(string $name = null)
 * @method static \LaravelKafka\Config\KafkaConfig config(string $name = null)
 * @method static void disconnect(string $name = null)
 * @method static void fake()
 * @method static bool isFake()
 *
 * @see \LaravelKafka\Manager\KafkaManager
 */
final class Kafka extends Facade
{
    /**
     * 返回 facade 背后的容器 key / 类名。
     *
     * 这里返回 `KafkaManager::class`，ServiceProvider 已在 `register()` 阶段
     * 用 `$this->app->alias(KafkaManager::class, 'kafka.manager')` 注册别名。
     *
     * @return string
     */
    protected static function getFacadeAccessor()
    {
        return KafkaManager::class;
    }

    /**
     * 启用 fake 模式（v0.2 引入，借鉴 mateusjunges 的 `Kafka::fake()`）。
     *
     * ## 流程
     *
     *  1. 创建（或拿到已存在的）单例 `FakeMessageStorage`
     *  2. 标记 `KafkaManager` 为 fake 模式
     *  3. 返回 `KafkaFake` 实例供业务方做断言
     *
     * ## 业务方用法
     *
     * ```php
     * $fake = Kafka::fake();   // 链式或赋值均可
     * Queue::push(new MyJob());
     * $fake->assertPushedOn('orders.created');
     *
     * // 或简写（静态方法直接调断言）
     * Kafka::assertPushedOn('orders.created');
     * ```
     *
     * ## 一次调用，多次使用
     *
     * `Kafka::fake()` 调一次后，fake 模式一直生效直到：
     * - 进程结束（容器销毁）
     * - `Kafka::disconnect()` 重建 manager（会丢失 fake 标志）
     *
     * 测试场景下一个 `setUp()` 调一次即可。
     *
     * @return KafkaFake fake 断言实例
     */
    public static function fake(): KafkaFake
    {
        $app = static::$app;
        $storage = $app->bound(FakeMessageStorage::class)
            ? $app->make(FakeMessageStorage::class)
            : new FakeMessageStorage();
        $app->instance(FakeMessageStorage::class, $storage);

        $app->make(KafkaManager::class)->fake();

        return new KafkaFake($storage);
    }
}
