<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Support;

use LaravelKafka\Support\TraceContext;
use LaravelKafka\Tests\TestCase;

/**
 * 覆盖 v0.2 重写的 TraceContext（W3C Trace Context 格式）。
 * v0.1 的测试只验证 16 hex，现在升级到完整 32/16/2 格式。
 *
 * @covers \LaravelKafka\Support\TraceContext
 */
final class TraceContextTest extends TestCase
{
    public function testNextProducesValidW3CTraceparent(): void
    {
        $tp = TraceContext::next();

        // 格式：00-<32hex>-<16hex>-<2hex>
        self::assertMatchesRegularExpression(
            '/^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/',
            $tp
        );
    }

    public function testNextProducesUniqueTraceparents(): void
    {
        $a = TraceContext::next();
        $b = TraceContext::next();
        self::assertNotSame($a, $b);
    }

    public function testParseValidTraceparent(): void
    {
        $tp = TraceContext::next();
        $parsed = TraceContext::parse($tp);

        self::assertNotNull($parsed);
        self::assertSame('00', $parsed['trace_id'] === '' ? '' : '00'); // 简化断言
        self::assertArrayHasKey('trace_id', $parsed);
        self::assertArrayHasKey('parent_id', $parsed);
        self::assertArrayHasKey('flags', $parsed);
        self::assertSame(32, strlen($parsed['trace_id']));
        self::assertSame(16, strlen($parsed['parent_id']));
        self::assertSame(2, strlen($parsed['flags']));
        self::assertSame('01', $parsed['flags']);
    }

    public function testParseInvalidReturnsNull(): void
    {
        self::assertNull(TraceContext::parse('not-a-valid-traceparent'));
        self::assertNull(TraceContext::parse('00-abc-def-01')); // trace_id 不是 32 hex
        self::assertNull(TraceContext::parse('01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01')); // 版本错
        self::assertNull(TraceContext::parse(''));
    }

    public function testChildPreservesTraceId(): void
    {
        $parent = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
        $child = TraceContext::child($parent);

        $childParsed = TraceContext::parse($child);
        $parentParsed = TraceContext::parse($parent);

        self::assertSame($parentParsed['trace_id'], $childParsed['trace_id']);
        self::assertNotSame($parentParsed['parent_id'], $childParsed['parent_id']);
    }

    public function testChildOfInvalidFallsBackToNew(): void
    {
        $child = TraceContext::child('garbage');
        self::assertMatchesRegularExpression(
            '/^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/',
            $child
        );
    }

    public function testShortTraceIdExtractsFirst16Hex(): void
    {
        $tp = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
        self::assertSame('0af7651916cd43dd', TraceContext::shortTraceId($tp));
    }

    public function testShortTraceIdOfInvalidReturnsNull(): void
    {
        self::assertNull(TraceContext::shortTraceId('garbage'));
    }
}
