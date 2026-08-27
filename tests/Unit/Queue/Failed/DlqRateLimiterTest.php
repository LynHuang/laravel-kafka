<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Queue\Failed;

use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Queue\Failed\DlqRateLimiter;
use LaravelKafka\Tests\TestCase;

/**
 * v0.3 Step 3: DlqRateLimiter 单元测试。
 *
 * @covers \LaravelKafka\Queue\Failed\DlqRateLimiter
 */
final class DlqRateLimiterTest extends TestCase
{
    public function testAllowsWithinLimit(): void
    {
        $limiter = new DlqRateLimiter(5);
        for ($i = 0; $i < 5; $i++) {
            self::assertTrue($limiter->allow());
        }
    }

    public function testDeniesAfterLimit(): void
    {
        $limiter = new DlqRateLimiter(3);
        self::assertTrue($limiter->allow());
        self::assertTrue($limiter->allow());
        self::assertTrue($limiter->allow());
        // 第 4 次拒绝
        self::assertFalse($limiter->allow());
        self::assertFalse($limiter->allow());
    }

    public function testCurrentCountTracks(): void
    {
        $limiter = new DlqRateLimiter(10);
        $limiter->allow();
        $limiter->allow();
        self::assertSame(2, $limiter->currentCount());
    }

    public function testMaxPerMinuteGetter(): void
    {
        $limiter = new DlqRateLimiter(42);
        self::assertSame(42, $limiter->maxPerMinute());
    }

    public function testConstructorRejectsZero(): void
    {
        $this->expectException(KafkaException::class);
        new DlqRateLimiter(0);
    }

    public function testConstructorRejectsNegative(): void
    {
        $this->expectException(KafkaException::class);
        new DlqRateLimiter(-1);
    }
}
