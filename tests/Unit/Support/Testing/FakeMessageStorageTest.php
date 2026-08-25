<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Support\Testing;

use LaravelKafka\Producer\Message;
use LaravelKafka\Support\Testing\FakeMessageStorage;
use LaravelKafka\Tests\TestCase;

/**
 * @covers \LaravelKafka\Support\Testing\FakeMessageStorage
 */
final class FakeMessageStorageTest extends TestCase
{
    public function testInitialState(): void
    {
        $storage = new FakeMessageStorage();
        $this->assertSame(0, $storage->count());
        $this->assertTrue($storage->isEmpty());
        $this->assertSame([], $storage->all());
    }

    public function testRecord(): void
    {
        $storage = new FakeMessageStorage();
        $msg = new Message('payload', ['k' => 'v']);

        $storage->record('orders.created', $msg);

        $this->assertSame(1, $storage->count());
        $this->assertFalse($storage->isEmpty());
        $this->assertSame([
            ['topic' => 'orders.created', 'message' => $msg],
        ], $storage->all());
    }

    public function testRecordMultiple(): void
    {
        $storage = new FakeMessageStorage();
        $msg1 = new Message('p1');
        $msg2 = new Message('p2');

        $storage->record('topic.a', $msg1);
        $storage->record('topic.b', $msg2);
        $storage->record('topic.a', $msg1);

        $this->assertSame(3, $storage->count());
    }

    public function testFlush(): void
    {
        $storage = new FakeMessageStorage();
        $storage->record('t', new Message('p'));
        $storage->record('t', new Message('p'));

        $storage->flush();

        $this->assertSame(0, $storage->count());
        $this->assertSame([], $storage->all());
    }
}
