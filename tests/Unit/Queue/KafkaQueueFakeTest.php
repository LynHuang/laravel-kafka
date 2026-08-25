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
        // 默认配置：非 fake 模式
        $manager = $this->app->make(KafkaManager::class);
        $this->assertFalse($manager->isFake());

        // fake storage 应该是空的（pushRaw 走真发路径，不写 storage）
        $storage = new FakeMessageStorage();
        $this->app->instance(FakeMessageStorage::class, $storage);

        // 期待：real producer 不可用，会抛 KafkaException（因为没真 Kafka 集群）
        $queue = $manager->connection();
        $this->expectException(\Throwable::class);
        $queue->pushRaw('test-payload', 'default');
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
