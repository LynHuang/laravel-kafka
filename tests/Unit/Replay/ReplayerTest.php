<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Replay;

use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Replay\Replayer;
use LaravelKafka\Tests\TestCase;

/**
 * v0.3 Step 4: Replayer 单元测试。
 *
 * @covers \LaravelKafka\Replay\Replayer
 */
final class ReplayerTest extends TestCase
{
    public function testParseWindowSuccess(): void
    {
        $replayer = new Replayer();
        $window = $replayer->parseWindow('-1h', 'now');
        self::assertArrayHasKey('from', $window);
        self::assertArrayHasKey('to', $window);
        self::assertLessThan($window['to'], $window['from']);
    }

    public function testParseWindowAbsoluteTimestamps(): void
    {
        $replayer = new Replayer();
        $window = $replayer->parseWindow('1700000000', '1700003600');
        self::assertSame(1700000000, $window['from']);
        self::assertSame(1700003600, $window['to']);
    }

    public function testParseWindowFromGreaterThanToThrows(): void
    {
        $replayer = new Replayer();
        $this->expectException(KafkaException::class);
        $this->expectExceptionMessage('must be <');

        $replayer->parseWindow('now', '-1h');
    }

    public function testParseWindowFromEqualsToThrows(): void
    {
        $replayer = new Replayer();
        $this->expectException(KafkaException::class);
        $this->expectExceptionMessage('must be <');

        $replayer->parseWindow('now', 'now');
    }
}
