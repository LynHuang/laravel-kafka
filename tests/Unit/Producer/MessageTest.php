<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Producer;

use LaravelKafka\Producer\Message;
use LaravelKafka\Tests\TestCase;

/**
 * @covers \LaravelKafka\Producer\Message
 */
final class MessageTest extends TestCase
{
    public function testBasicAccessors(): void
    {
        $msg = new Message('payload-value', ['k' => 'v'], 'the-key', 3, 1234567890);

        self::assertSame('payload-value', $msg->payload());
        self::assertSame(['k' => 'v'], $msg->headers());
        self::assertSame('v', $msg->header('k'));
        self::assertSame('the-key', $msg->key());
        self::assertSame(3, $msg->partition());
        self::assertSame(1234567890, $msg->timestampMs());
    }

    public function testHeaderDefault(): void
    {
        $msg = new Message('payload');
        self::assertNull($msg->header('missing'));
        self::assertSame('fallback', $msg->header('missing', 'fallback'));
    }

    public function testWithHeadersIsImmutable(): void
    {
        $msg = new Message('payload', ['a' => '1']);
        $next = $msg->withHeaders(['b' => '2']);

        // 原对象没变
        self::assertSame(['a' => '1'], $msg->headers());
        // 新对象是合并结果
        self::assertSame(['a' => '1', 'b' => '2'], $next->headers());
    }

    public function testWithKeyIsImmutable(): void
    {
        $msg = new Message('payload', [], 'old-key');
        $next = $msg->withKey('new-key');

        self::assertSame('old-key', $msg->key());
        self::assertSame('new-key', $next->key());
    }

    public function testWithHeaderSingle(): void
    {
        $msg = (new Message('payload'))->withHeader('x-trace', 'abc');
        self::assertSame('abc', $msg->header('x-trace'));
    }
}
