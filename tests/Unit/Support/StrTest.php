<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Support;

use LaravelKafka\Support\Str;
use LaravelKafka\Tests\TestCase;

/**
 * @covers \LaravelKafka\Support\Str
 */
final class StrTest extends TestCase
{
    public function testTruncateShorterThanMax(): void
    {
        self::assertSame('hello', Str::truncate('hello', 10));
    }

    public function testTruncateLongerThanMax(): void
    {
        self::assertSame('hell...', Str::truncate('hello world', 7));
    }

    public function testTruncateExactLength(): void
    {
        self::assertSame('hello', Str::truncate('hello', 5));
    }

    public function testMaskBasic(): void
    {
        // 'hello world' (11 字符): head='he', tail='ld', middle=11-2-2=7 个 *
        self::assertSame('he*******ld', Str::mask('hello world', 2, 2));
    }

    public function testMaskShorterThanVisible(): void
    {
        // 'hi' (2 字符) <= visibleStart+visibleEnd (4) → 全部 mask
        self::assertSame('**', Str::mask('hi', 2, 2));
    }

    public function testIsUuidValid(): void
    {
        self::assertTrue(Str::isUuid('550e8400-e29b-41d4-a716-446655440000'));
    }

    public function testIsUuidInvalid(): void
    {
        self::assertFalse(Str::isUuid('not-a-uuid'));
        self::assertFalse(Str::isUuid('550e8400-e29b-41d4-a716'));
    }
}
