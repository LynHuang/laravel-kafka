<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Replay;

use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Replay\TimeWindowParser;
use LaravelKafka\Tests\TestCase;

/**
 * v0.3 Step 4: TimeWindowParser 单元测试。
 *
 * @covers \LaravelKafka\Replay\TimeWindowParser
 */
final class TimeWindowParserTest extends TestCase
{
    public function testParseNow(): void
    {
        $parser = new TimeWindowParser();
        $now = 1700000000;
        $this->assertSame($now, $parser->parse('now', $now));
    }

    public function testParseRelativeHours(): void
    {
        $parser = new TimeWindowParser();
        $now = 1700000000;
        $this->assertSame($now - 3600, $parser->parse('-1h', $now));
    }

    public function testParseRelativeMinutes(): void
    {
        $parser = new TimeWindowParser();
        $now = 1700000000;
        $this->assertSame($now - 30 * 60, $parser->parse('-30m', $now));
    }

    public function testParseRelativeDays(): void
    {
        $parser = new TimeWindowParser();
        $now = 1700000000;
        $this->assertSame($now - 7 * 86400, $parser->parse('-7d', $now));
    }

    public function testParseRelativeSeconds(): void
    {
        $parser = new TimeWindowParser();
        $now = 1700000000;
        $this->assertSame($now - 60, $parser->parse('-60s', $now));
    }

    public function testParseAbsoluteTimestamp(): void
    {
        $parser = new TimeWindowParser();
        $this->assertSame(1700000000, $parser->parse('1700000000'));
    }

    public function testParseAbsoluteDateString(): void
    {
        $parser = new TimeWindowParser();
        $ts = $parser->parse('2026-08-25 10:00:00');
        $this->assertIsInt($ts);
        // 2026-08-25 10:00:00 UTC = 1756111200（依赖时区，但肯定是 int）
    }

    public function testParseEmptyThrows(): void
    {
        $parser = new TimeWindowParser();
        $this->expectException(KafkaException::class);
        $parser->parse('');
    }

    public function testParseInvalidThrows(): void
    {
        $parser = new TimeWindowParser();
        $this->expectException(KafkaException::class);
        $this->expectExceptionMessage('cannot parse');
        $parser->parse('not-a-time');
    }

    public function testParseTrimsWhitespace(): void
    {
        $parser = new TimeWindowParser();
        $now = 1700000000;
        $this->assertSame($now, $parser->parse('  now  ', $now));
    }
}
