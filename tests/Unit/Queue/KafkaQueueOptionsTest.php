<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Queue;

use LaravelKafka\Manager\KafkaManager;
use LaravelKafka\Support\Testing\FakeMessageStorage;
use LaravelKafka\Tests\TestCase;

/**
 * Step 3 (multi-topic 路由) + Step 4 (Key Routing) 的集成测试。
 *
 * 通过 Kafka::fake() 拦截 pushRaw，记录到 FakeMessageStorage，
 * 然后断言 topic 与 key 的映射。
 *
 * @covers \LaravelKafka\Queue\KafkaQueue
 */
final class KafkaQueueOptionsTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        // 启用 fake 模式
        $this->app->make(KafkaManager::class)->fake();

        // 注入干净 storage
        $this->app->instance(FakeMessageStorage::class, new FakeMessageStorage());
    }

    // ──────── Step 3：Multi-topic 路由 ────────

    public function testResolveTopicFallsBackToQueueName(): void
    {
        $this->pushAndAssert(
            fn($q) => $q->pushRaw('p', 'reports'),
            function (array $records) {
                $this->assertCount(1, $records);
                $this->assertSame('reports', $records[0]['topic']);
            }
        );
    }

    public function testResolveTopicFallsBackToDefaultWhenNoQueue(): void
    {
        $this->pushAndAssert(
            fn($q) => $q->pushRaw('p'),
            function (array $records) {
                $this->assertCount(1, $records);
                $this->assertSame('laravel-jobs-test', $records[0]['topic']);
            }
        );
    }

    public function testOptionsTopicOverridesEverything(): void
    {
        $this->pushAndAssert(
            fn($q) => $q->pushRaw('p', 'reports', ['topic' => 'audit.events']),
            function (array $records) {
                $this->assertCount(1, $records);
                $this->assertSame('audit.events', $records[0]['topic']);
            }
        );
    }

    public function testEmptyOptionsTopicFallsBack(): void
    {
        $this->pushAndAssert(
            fn($q) => $q->pushRaw('p', 'reports', ['topic' => '']),
            function (array $records) {
                $this->assertCount(1, $records);
                $this->assertSame('reports', $records[0]['topic']);
            }
        );
    }

    // ──────── Step 4：Key Routing ────────

    public function testPushWithoutKeyProducesNullKey(): void
    {
        $this->pushAndAssert(
            fn($q) => $q->pushRaw('p', 'orders'),
            function (array $records) {
                $this->assertCount(1, $records);
                $this->assertNull($records[0]['message']->key());
            }
        );
    }

    public function testPushWithOptionsKey(): void
    {
        $this->pushAndAssert(
            fn($q) => $q->pushRaw('p', 'orders', ['key' => 'user-42']),
            function (array $records) {
                $this->assertCount(1, $records);
                $this->assertSame('user-42', $records[0]['message']->key());
            }
        );
    }

    public function testPushWithKeyAsFourthParameter(): void
    {
        // KafkaQueue::push($job, $data, $queue, $key) 是 v0.2 新增的第 4 参数
        $this->pushAndAssert(
            fn($q) => $q->push(function () {}, 'payload', 'orders', 'user-99'),
            function (array $records) {
                $this->assertCount(1, $records);
                $this->assertSame('user-99', $records[0]['message']->key());
            }
        );
    }

    public function testPushFourthParameterDefaultsToNull(): void
    {
        // 不传第 4 参数 → 行为与 v0.1 一致（key = null）
        $this->pushAndAssert(
            fn($q) => $q->push(function () {}, 'payload', 'orders'),
            function (array $records) {
                $this->assertCount(1, $records);
                $this->assertNull($records[0]['message']->key());
            }
        );
    }

    // ──────── Step 5：Header Trace ────────

    public function testPushWritesW3CTraceparent(): void
    {
        $this->pushAndAssert(
            fn($q) => $q->pushRaw('p', 'orders'),
            function (array $records) {
                $tp = $records[0]['message']->header('traceparent');
                $this->assertNotNull($tp);
                $this->assertMatchesRegularExpression(
                    '/^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/',
                    $tp
                );
            }
        );
    }

    public function testPushAlsoWritesShortXTraceIdForBackwardCompat(): void
    {
        $this->pushAndAssert(
            fn($q) => $q->pushRaw('p', 'orders'),
            function (array $records) {
                $short = $records[0]['message']->header('x-trace-id');
                $tp = $records[0]['message']->header('traceparent');
                $this->assertNotNull($short);
                $this->assertSame(16, strlen($short));
                // x-trace-id 应是 traceparent 的 trace-id 前 16 hex
                $this->assertSame(substr(explode('-', $tp)[1], 0, 16), $short);
            }
        );
    }

    public function testPushRespectsProvidedTraceparent(): void
    {
        $tp = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
        $this->pushAndAssert(
            fn($q) => $q->pushRaw('p', 'orders', ['traceparent' => $tp]),
            function (array $records) use ($tp) {
                $this->assertSame($tp, $records[0]['message']->header('traceparent'));
            }
        );
    }

    private function pushAndAssert(callable $push, callable $assert): void
    {
        $manager = $this->app->make(KafkaManager::class);
        $queue = $manager->connection();
        $push($queue);
        $storage = $this->app->make(FakeMessageStorage::class);
        $assert($storage->all());
    }
}
