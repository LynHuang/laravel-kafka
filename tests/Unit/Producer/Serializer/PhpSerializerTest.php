<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Producer\Serializer;

use LaravelKafka\Exceptions\SerializationException;
use LaravelKafka\Producer\Serializer\PhpSerializer;
use LaravelKafka\Tests\TestCase;

/**
 * @covers \LaravelKafka\Producer\Serializer\PhpSerializer
 */
final class PhpSerializerTest extends TestCase
{
    public function testEncodeDecodeScalar(): void
    {
        $s = new PhpSerializer();
        $encoded = $s->encode('hello');
        $this->assertSame('hello', $s->decode($encoded));
    }

    public function testEncodeDecodeArray(): void
    {
        $s = new PhpSerializer();
        $data = ['a' => 1, 'b' => ['nested' => true]];
        $this->assertSame($data, $s->decode($s->encode($data)));
    }

    public function testEncodeDecodeObject(): void
    {
        $s = new PhpSerializer();
        $obj = new \stdClass();
        $obj->x = 42;
        $encoded = $s->encode($obj);
        /** @var \stdClass $decoded */
        $decoded = $s->decode($encoded);
        $this->assertSame(42, $decoded->x);
    }

    public function testDecodeEmptyString(): void
    {
        $s = new PhpSerializer();
        $this->assertNull($s->decode(''));
    }

    public function testDecodeInvalidPayloadThrows(): void
    {
        $s = new PhpSerializer();
        $this->expectException(SerializationException::class);
        $s->decode('not-a-valid-serialized-string');
    }

    public function testName(): void
    {
        $s = new PhpSerializer();
        $this->assertSame('php', $s->name());
    }
}
