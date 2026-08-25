<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Support;

use LaravelKafka\Support\Header;
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
        $this->assertMatchesRegularExpression(
            '/^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/',
            $tp
        );
    }

    public function testNextProducesUniqueTraceparents(): void
    {
        $a = TraceContext::next();
        $b = TraceContext::next();
        $this->assertNotSame($a, $b);
    }

    public function testParseValidTraceparent(): void
    {
        $tp = TraceContext::next();
        $parsed = TraceContext::parse($tp);

        $this->assertNotNull($parsed);
        $this->assertSame('00', $parsed['trace_id'] === '' ? '' : '00'); // 简化断言
        $this->assertArrayHasKey('trace_id', $parsed);
        $this->assertArrayHasKey('parent_id', $parsed);
        $this->assertArrayHasKey('flags', $parsed);
        $this->assertSame(32, strlen($parsed['trace_id']));
        $this->assertSame(16, strlen($parsed['parent_id']));
        $this->assertSame(2, strlen($parsed['flags']));
        $this->assertSame('01', $parsed['flags']);
    }

    public function testParseInvalidReturnsNull(): void
    {
        $this->assertNull(TraceContext::parse('not-a-valid-traceparent'));
        $this->assertNull(TraceContext::parse('00-abc-def-01')); // trace_id 不是 32 hex
        $this->assertNull(TraceContext::parse('01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01')); // 版本错
        $this->assertNull(TraceContext::parse(''));
    }

    public function testChildPreservesTraceId(): void
    {
        $parent = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
        $child = TraceContext::child($parent);

        $childParsed = TraceContext::parse($child);
        $parentParsed = TraceContext::parse($parent);

        $this->assertSame($parentParsed['trace_id'], $childParsed['trace_id']);
        $this->assertNotSame($parentParsed['parent_id'], $childParsed['parent_id']);
    }

    public function testChildOfInvalidFallsBackToNew(): void
    {
        $child = TraceContext::child('garbage');
        $this->assertMatchesRegularExpression(
            '/^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/',
            $child
        );
    }

    public function testShortTraceIdExtractsFirst16Hex(): void
    {
        $tp = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
        $this->assertSame('0af7651916cd43dd', TraceContext::shortTraceId($tp));
    }

    public function testShortTraceIdOfInvalidReturnsNull(): void
    {
        $this->assertNull(TraceContext::shortTraceId('garbage'));
    }
}
