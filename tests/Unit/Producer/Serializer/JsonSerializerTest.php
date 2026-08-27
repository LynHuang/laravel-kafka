<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Producer\Serializer;

use LaravelKafka\Exceptions\SerializationException;
use LaravelKafka\Producer\Serializer\JsonSerializer;
use LaravelKafka\Tests\TestCase;

/**
 * @covers \LaravelKafka\Producer\Serializer\JsonSerializer
 */
final class JsonSerializerTest extends TestCase
{
    public function testEncodeDecodeArray(): void
    {
        $s = new JsonSerializer();
        $data = ['a' => 1, 'b' => '中文'];
        $encoded = $s->encode($data);
        self::assertStringContainsString('中文', $encoded); // UNESCAPED_UNICODE
        self::assertSame($data, $s->decode($encoded));
    }

    public function testEncodeKeepsUnicode(): void
    {
        $s = new JsonSerializer();
        $encoded = $s->encode(['message' => '你好世界']);
        self::assertStringContainsString('你好世界', $encoded);
    }

    public function testEncodeKeepsSlashes(): void
    {
        $s = new JsonSerializer();
        $encoded = $s->encode(['url' => 'https://example.com/foo']);
        self::assertStringContainsString('https://example.com/foo', $encoded);
    }

    public function testDecodeEmptyString(): void
    {
        $s = new JsonSerializer();
        self::assertNull($s->decode(''));
    }

    public function testDecodeInvalidJsonThrows(): void
    {
        $s = new JsonSerializer();
        $this->expectException(SerializationException::class);
        $s->decode('{not-json}');
    }

    public function testName(): void
    {
        $s = new JsonSerializer();
        self::assertSame('json', $s->name());
    }
}
