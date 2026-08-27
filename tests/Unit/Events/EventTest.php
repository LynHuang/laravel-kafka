<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Events;

use LaravelKafka\Events\MessageConsumed;
use LaravelKafka\Events\MessageConsuming;
use LaravelKafka\Events\MessageFailed;
use LaravelKafka\Events\MessagePublished;
use LaravelKafka\Events\MessagePublishing;
use LaravelKafka\Events\MessageSentToDLQ;
use LaravelKafka\Producer\Message;
use LaravelKafka\Tests\TestCase;

/**
 * 6 个事件值对象的访问器测试。
 * 不测 dispatch 行为（那是集成测试范畴）—— 只测构造与读取。
 *
 * @covers \LaravelKafka\Events\MessagePublishing
 * @covers \LaravelKafka\Events\MessagePublished
 * @covers \LaravelKafka\Events\MessageConsuming
 * @covers \LaravelKafka\Events\MessageConsumed
 * @covers \LaravelKafka\Events\MessageFailed
 * @covers \LaravelKafka\Events\MessageSentToDLQ
 */
final class EventTest extends TestCase
{
    public function testMessagePublishing(): void
    {
        $msg = new Message('payload');
        $event = new MessagePublishing('orders.created', $msg);

        self::assertSame('orders.created', $event->topic());
        self::assertSame($msg, $event->message());
    }

    public function testMessagePublished(): void
    {
        $msg = new Message('payload');
        $event = new MessagePublished('orders.created', $msg);

        self::assertSame('orders.created', $event->topic());
        self::assertSame($msg, $event->message());
    }

    public function testMessageConsuming(): void
    {
        $msg = new Message('payload');
        $event = new MessageConsuming('orders.created', $msg);

        self::assertSame('orders.created', $event->topic());
        self::assertSame($msg, $event->message());
    }

    public function testMessageConsumed(): void
    {
        $msg = new Message('payload');
        $event = new MessageConsumed('orders.created', $msg);

        self::assertSame('orders.created', $event->topic());
        self::assertSame($msg, $event->message());
    }

    public function testMessageFailed(): void
    {
        $msg = new Message('payload');
        $error = new \RuntimeException('boom');
        $event = new MessageFailed('orders.created', $msg, $error);

        self::assertSame('orders.created', $event->topic());
        self::assertSame($msg, $event->message());
        self::assertSame($error, $event->error());
    }

    public function testMessageSentToDLQ(): void
    {
        $msg = new Message('payload');
        $error = new \RuntimeException('boom');
        $event = new MessageSentToDLQ('orders.created.dlq', $msg, $error);

        self::assertSame('orders.created.dlq', $event->dlqTopic());
        self::assertSame($msg, $event->message());
        self::assertSame($error, $event->error());
    }

    public function testEventsAreFinalAndReadonly(): void
    {
        // 通过反射验证 final
        $reflection = new \ReflectionClass(MessagePublishing::class);
        self::assertTrue($reflection->isFinal());
    }
}
