<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Support\Testing;

use LaravelKafka\Producer\Message;
use LaravelKafka\Support\Testing\FakeMessageStorage;
use LaravelKafka\Support\Testing\KafkaFake;
use LaravelKafka\Tests\TestCase;
use PHPUnit\Framework\AssertionFailedError;

/**
 * @covers \LaravelKafka\Support\Testing\KafkaFake
 */
final class KafkaFakeTest extends TestCase
{
    public function testAssertPushedOnFailsWhenStorageEmpty(): void
    {
        $fake = $this->makeFake();
        $this->expectException(AssertionFailedError::class);
        $fake->assertPushedOn('orders.created');
    }

    public function testAssertPushedSucceedsWhenMatch(): void
    {
        $storage = new FakeMessageStorage();
        $msg = new Message('hello');
        $storage->record('orders', $msg);
        $fake = new KafkaFake($storage);

        $fake->assertPushedOn('orders', function (string $topic, Message $m) use ($msg): bool {
            return $m === $msg;
        });
        self::assertTrue(true);
    }

    public function testAssertNothingPushed(): void
    {
        $fake = $this->makeFake();
        $fake->assertNothingPushed();
        self::assertTrue(true);
    }

    public function testAssertNothingPushedFailsWhenOnePublished(): void
    {
        $storage = new FakeMessageStorage();
        $storage->record('topic', new Message('p'));
        $fake = new KafkaFake($storage);

        $this->expectException(AssertionFailedError::class);
        $fake->assertNothingPushed();
    }

    public function testAssertPushedOnTimesExactCount(): void
    {
        $storage = new FakeMessageStorage();
        $storage->record('orders.created', new Message('a'));
        $storage->record('orders.created', new Message('b'));
        $storage->record('orders.paid', new Message('c'));
        $fake = new KafkaFake($storage);

        $fake->assertPushedOnTimes('orders.created', 2);
        self::assertTrue(true);
    }

    public function testAssertPushedOnTimesFailsOnWrongCount(): void
    {
        $storage = new FakeMessageStorage();
        $storage->record('orders.created', new Message('a'));
        $fake = new KafkaFake($storage);

        $this->expectException(AssertionFailedError::class);
        $fake->assertPushedOnTimes('orders.created', 5);
    }

    public function testCallbackFilteringByPayload(): void
    {
        $storage = new FakeMessageStorage();
        $storage->record('orders', new Message('hello'));
        $storage->record('orders', new Message('world'));
        $fake = new KafkaFake($storage);

        $fake->assertPushed('orders', function (string $topic, Message $m): bool {
            return $m->payload() === 'world';
        });
        self::assertTrue(true);
    }

    public function testAssertPushedFailsWhenNoMatch(): void
    {
        $storage = new FakeMessageStorage();
        $storage->record('orders', new Message('hello'));
        $fake = new KafkaFake($storage);

        $this->expectException(AssertionFailedError::class);
        $fake->assertPushed('orders', function (string $topic, Message $m): bool {
            return $m->payload() === 'nonexistent';
        });
    }
    private function makeFake(): KafkaFake
    {
        return new KafkaFake(new FakeMessageStorage());
    }
}
