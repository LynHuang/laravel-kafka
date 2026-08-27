<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Queue;

use LaravelKafka\Manager\KafkaManager;
use LaravelKafka\Producer\Message;
use LaravelKafka\Queue\KafkaQueue;
use LaravelKafka\Support\Testing\FakeMessageStorage;
use LaravelKafka\Tests\TestCase;

/**
 * @covers \LaravelKafka\Queue\KafkaQueue
 */
final class KafkaQueueFakeTest extends TestCase
{
    public function testPushRawInNormalModeGoesToRealProducer(): void
    {
        // v0.4.7 修复: 之前测试假设 "real producer 不可用会抛 KafkaException",
        // 但 CI runner (services.kafka) + 业务方本机都有 Kafka, 真发成功不抛, test fail.
        //
        // 核心断言改为: non-fake 模式下, pushRaw 走 Producer.send 真发路径,
        // **不**写 FakeMessageStorage (storage 计数为 0).
        //
        // 接受两种环境:
        //  - 有 Kafka: 真发成功, 返回 partition 编号 (int)
        //  - 无 Kafka: 抛 KafkaException (本机/CI 没 Kafka 集群)
        // 两种情况都验证 storage 没被写.
        $manager = $this->app->make(KafkaManager::class);
        $this->assertFalse($manager->isFake());

        // fake storage 应该是空的（pushRaw 走真发路径，不写 storage）
        $storage = new FakeMessageStorage();
        $this->app->instance(FakeMessageStorage::class, $storage);

        $queue = $manager->connection();

        try {
            $result = $queue->pushRaw('test-payload', 'default');
            // 真发成功: 返回 partition 编号
            $this->assertIsInt($result);
        } catch (\LaravelKafka\Exceptions\KafkaException $e) {
            // 本机/CI 无 Kafka 集群: 接受抛 KafkaException
            $this->addToAssertionCount(1);
        }

        // 关键断言: 无论真发成功/失败, non-fake 模式不应写 storage
        $this->assertSame(0, $storage->count(), 'non-fake 模式不应写 fake storage');
    }

    public function testPushRawInFakeModeRecordsToStorage(): void
    {
        // 准备：fake 模式
        $manager = $this->app->make(KafkaManager::class);
        $manager->fake();

        $storage = new FakeMessageStorage();
        $this->app->instance(FakeMessageStorage::class, $storage);

        // 准备：创建 KafkaQueue（用 connection() 拿，因为我们不是直接 new）
        $queue = $manager->connection();

        // 执行：pushRaw(queue=null) 走 defaultTopic 兜底，得到 'laravel-jobs-test'
        $result = $queue->pushRaw('test-payload', null);
        $this->assertSame(0, $result);

        // 验证：storage 收到 1 条
        $this->assertSame(1, $storage->count());
        $records = $storage->all();
        $this->assertSame('laravel-jobs-test', $records[0]['topic']);
        $this->assertSame('test-payload', $records[0]['message']->payload());
    }

    public function testPushResolvesTopicBeforeFakeRecords(): void
    {
        $manager = $this->app->make(KafkaManager::class);
        $manager->fake();

        $storage = new FakeMessageStorage();
        $this->app->instance(FakeMessageStorage::class, $storage);

        $queue = $manager->connection();

        // 不传 queue，落到 defaultTopic
        $queue->pushRaw('p', null);
        $records = $storage->all();
        $this->assertSame('laravel-jobs-test', $records[0]['topic']);

        // 传 queue 字符串
        $queue->pushRaw('p', 'emails');
        $records = $storage->all();
        $this->assertSame('emails', $records[1]['topic']);
    }

    public function testFakeModeIsControllablePerManagerInstance(): void
    {
        // 第一次拿：非 fake
        $manager1 = $this->app->make(KafkaManager::class);
        $this->assertFalse($manager1->isFake());

        // 切 fake
        $manager1->fake();
        $this->assertTrue($manager1->isFake());

        // 因为是 singleton，拿到的还是同一个，状态保持
        $manager2 = $this->app->make(KafkaManager::class);
        $this->assertTrue($manager2->isFake());
    }
}
