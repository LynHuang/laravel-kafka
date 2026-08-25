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

        $this->assertSame('payload-value', $msg->payload());
        $this->assertSame(['k' => 'v'], $msg->headers());
        $this->assertSame('v', $msg->header('k'));
        $this->assertSame('the-key', $msg->key());
        $this->assertSame(3, $msg->partition());
        $this->assertSame(1234567890, $msg->timestampMs());
    }

    public function testHeaderDefault(): void
    {
        $msg = new Message('payload');
        $this->assertNull($msg->header('missing'));
        $this->assertSame('fallback', $msg->header('missing', 'fallback'));
    }

    public function testWithHeadersIsImmutable(): void
    {
        $msg = new Message('payload', ['a' => '1']);
        $next = $msg->withHeaders(['b' => '2']);

        // 原对象没变
        $this->assertSame(['a' => '1'], $msg->headers());
        // 新对象是合并结果
        $this->assertSame(['a' => '1', 'b' => '2'], $next->headers());
    }

    public function testWithKeyIsImmutable(): void
    {
        $msg = new Message('payload', [], 'old-key');
        $next = $msg->withKey('new-key');

        $this->assertSame('old-key', $msg->key());
        $this->assertSame('new-key', $next->key());
    }

    public function testWithHeaderSingle(): void
    {
        $msg = (new Message('payload'))->withHeader('x-trace', 'abc');
        $this->assertSame('abc', $msg->header('x-trace'));
    }
}
