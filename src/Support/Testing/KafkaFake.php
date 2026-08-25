<?php

declare(strict_types=1);

namespace LaravelKafka\Support\Testing;

use LaravelKafka\Producer\Message;
use PHPUnit\Framework\Assert as PHPUnit;

/**
 * KafkaFake 断言门面。
 *
 * ## 业务方用法
 *
 * ```php
 * use LaravelKafka\Facades\Kafka;
 * use LaravelKafka\Producer\Message;
 *
 * public function test_order_creation_publishes_event(): void
 * {
 *     Kafka::fake();   // 启用 fake 模式
 *
 *     $this->post('/orders', [...]);
 *
 *     // 断言 1：指定 topic 至少 1 条
 *     Kafka::assertPushedOn('orders.created');
 *
 *     // 断言 2：带 callback 验 payload
 *     Kafka::assertPushedOn('orders.created', function (string $topic, Message $msg): bool {
 *         return str_contains($msg->payload(), 'amount=100');
 *     });
 *
 *     // 断言 3：指定 topic 恰好 N 条
 *     Kafka::assertPushedOnTimes('orders.created', 1);
 *
 *     // 断言 4：没有任何消息被发到某 topic
 *     Kafka::assertNothingPushed();
 * }
 * ```
 *
 * ## 与 mateusjunges 的差异
 *
 * | 我们 | mateusjunges | 含义 |
 * | --- | --- | --- |
 * | `assertPushed` | `assertPublished` | 至少 1 条匹配 |
 * | `assertPushedOn` | `assertPublishedOn` | 在指定 topic 至少 1 条 |
 * | `assertPushedTimes` | `assertPublishedTimes` | 恰好 N 条 |
 * | `assertPushedOnTimes` | `assertPublishedOnTimes` | 在指定 topic 恰好 N 条 |
 * | `assertNothingPushed` | `assertNothingPublished` | 没有 |
 *
 * 命名差异原因：我们的语义基于 Laravel Queue 驱动，"pushed" 比 "published" 更准。
 *
 * ## 失败时行为
 *
 * 失败抛 `PHPUnit\Framework\AssertionFailedError`，PHPUnit 测试框架自动捕获。
 * 业务方测试报告会显示 "Failed asserting that ..."。
 *
 * @see \LaravelKafka\Facades\Kafka::fake()
 * @see \LaravelKafka\Support\Testing\FakeMessageStorage
 */
final class KafkaFake
{
    /**
     * fake 消息存储句柄。
     *
     * @var FakeMessageStorage
     */
    private FakeMessageStorage $storage;

    /**
     * 构造时由 `Kafka::fake()` 注入单例 storage。
     *
     * @param FakeMessageStorage $storage 已实例化的存储
     */
    public function __construct(FakeMessageStorage $storage)
    {
        $this->storage = $storage;
    }

    /**
     * 断言至少有一条 push 调用匹配。
     *
     * @param string|null  $topic    期望的物理 topic 名；`null` 表示任意 topic
     * @param callable|null $callback 真值测试回调，签名 `function (string $topic, Message $message): bool`；
     *                              返回 `true` 算匹配，`false` 算不匹配
     * @return void
     * @throws \PHPUnit\Framework\AssertionFailedError 没有匹配时
     */
    public function assertPushed(?string $topic = null, ?callable $callback = null): void
    {
        $count = $this->matching($topic, $callback);
        PHPUnit::assertGreaterThan(
            0,
            $count,
            sprintf(
                'Expected at least one Kafka message to be published%s, but %d were published.',
                $topic !== null ? ' on topic [' . $topic . ']' : '',
                $this->storage->count()
            )
        );
    }

    /**
     * 断言恰好 N 条 push 调用匹配。
     *
     * @param int           $times    期望的匹配数
     * @param string|null   $topic    期望的物理 topic 名；`null` 表示任意
     * @param callable|null $callback 真值测试回调
     * @return void
     * @throws \PHPUnit\Framework\AssertionFailedError 实际匹配数 ≠ $times 时
     */
    public function assertPushedTimes(int $times, ?string $topic = null, ?callable $callback = null): void
    {
        $count = $this->matching($topic, $callback);
        PHPUnit::assertSame(
            $times,
            $count,
            sprintf(
                'Expected exactly %d Kafka message(s) to be published%s, but %d were published.',
                $times,
                $topic !== null ? ' on topic [' . $topic . ']' : '',
                $count
            )
        );
    }

    /**
     * 断言指定 topic 上至少有一条 push。
     *
     * @param string         $topic    期望的物理 topic 名
     * @param callable|null  $callback 真值测试回调
     * @return void
     * @throws \PHPUnit\Framework\AssertionFailedError 没有匹配时
     */
    public function assertPushedOn(string $topic, ?callable $callback = null): void
    {
        $this->assertPushed($topic, $callback);
    }

    /**
     * 断言指定 topic 上恰好 N 条 push。
     *
     * @param string         $topic    期望的物理 topic 名
     * @param int            $times    期望的匹配数
     * @param callable|null  $callback 真值测试回调
     * @return void
     * @throws \PHPUnit\Framework\AssertionFailedError 实际匹配数 ≠ $times 时
     */
    public function assertPushedOnTimes(string $topic, int $times, ?callable $callback = null): void
    {
        $this->assertPushedTimes($times, $topic, $callback);
    }

    /**
     * 断言没有任何 push 调用。
     *
     * 典型用法：业务方期望某条件下消息**不**该发，用此断言。
     *
     * @return void
     * @throws \PHPUnit\Framework\AssertionFailedError 有 push 时
     */
    public function assertNothingPushed(): void
    {
        PHPUnit::assertSame(
            0,
            $this->storage->count(),
            sprintf(
                'Expected no Kafka messages to be published, but %d were published.',
                $this->storage->count()
            )
        );
    }

    /**
     * 内部：遍历 storage 统计匹配数。
     *
     * 匹配规则：topic 匹配（若指定）**且** callback 返回 true（若提供）。
     *
     * @param string|null   $topic    期望的 topic
     * @param callable|null $callback 真值测试回调
     * @return int 匹配的记录数
     */
    private function matching(?string $topic, ?callable $callback): int
    {
        $count = 0;
        foreach ($this->storage->all() as $record) {
            // 1. topic 过滤
            if ($topic !== null && $record['topic'] !== $topic) {
                continue;
            }
            // 2. callback 过滤
            if ($callback !== null && $callback($record['topic'], $record['message']) !== true) {
                continue;
            }
            $count++;
        }
        return $count;
    }
}
