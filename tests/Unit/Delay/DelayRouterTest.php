<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Delay;

use LaravelKafka\Delay\DelayRouter;
use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Tests\TestCase;

/**
 * v0.3 Step 2: 时间轮分层路由 DelayRouter 单元测试。
 *
 * @covers \LaravelKafka\Delay\DelayRouter
 */
final class DelayRouterTest extends TestCase
{
    public function testRouteToFiveSecondTier(): void
    {
        $router = new DelayRouter([5, 30, 60, 300, 1800, 3600, 86400]);
        $result = $router->route(3);
        self::assertSame('delay-5s', $result['topic']);
        self::assertSame(5, $result['tier']);
    }

    public function testRouteToThirtySecondTier(): void
    {
        $router = new DelayRouter([5, 30, 60, 300, 1800, 3600, 86400]);
        $result = $router->route(15);
        self::assertSame('delay-30s', $result['topic']);
        self::assertSame(30, $result['tier']);
    }

    public function testRouteToExactBoundary(): void
    {
        $router = new DelayRouter([5, 30, 60, 300, 1800, 3600, 86400]);
        // 正好等于 tier 边界 → 落到该 tier
        $result = $router->route(60);
        self::assertSame('delay-60s', $result['topic']);
        self::assertSame(60, $result['tier']);
    }

    public function testRouteToMaxTierWhenExceeding(): void
    {
        $router = new DelayRouter([5, 30, 60, 300, 1800, 3600, 86400]);
        // 超过最大 tier（86400 = 1 day）→ 用最大 tier
        $result = $router->route(90000);
        self::assertSame('delay-86400s', $result['topic']);
        self::assertSame(86400, $result['tier']);
    }

    public function testRouteThrowsOnZeroOrNegative(): void
    {
        $router = new DelayRouter([5, 30, 60]);

        $this->expectException(KafkaException::class);
        $this->expectExceptionMessage('DelayRouter delay must be > 0, got: 0');

        $router->route(0);
    }

    public function testCustomTopicPrefix(): void
    {
        $router = new DelayRouter([5, 30, 60], 'kafka-delay');
        $result = $router->route(10);
        self::assertSame('kafka-delay-30s', $result['topic']);
    }

    public function testAllTopicsReturnsAllTiers(): void
    {
        $router = new DelayRouter([5, 30, 60]);
        self::assertSame(['delay-5s', 'delay-30s', 'delay-60s'], $router->allTopics());
    }

    public function testParseTierFromTopicName(): void
    {
        $router = new DelayRouter([5, 30, 60]);
        self::assertSame(5, $router->parseTier('delay-5s'));
        self::assertSame(30, $router->parseTier('delay-30s'));
        self::assertSame(60, $router->parseTier('delay-60s'));
        self::assertSame(0, $router->parseTier('orders-events'));
        self::assertSame(0, $router->parseTier('delay-5'));   // 缺 's'
        self::assertSame(0, $router->parseTier('delay-Xs'));   // 非数字
    }

    public function testConstructorValidatesAscendingOrder(): void
    {
        $this->expectException(KafkaException::class);
        $this->expectExceptionMessage('must be in ascending order');

        new DelayRouter([60, 30, 5]);
    }

    public function testConstructorValidatesPositiveTiers(): void
    {
        $this->expectException(KafkaException::class);
        $this->expectExceptionMessage('tier must be > 0');

        // 升序但含 0（绕开 ascending 检查）
        new DelayRouter([0, 5, 30]);
    }
}
