<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Exceptions;

use LaravelKafka\Exceptions\DlqException;
use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Exceptions\SerializationException;
use LaravelKafka\Tests\TestCase;
use RuntimeException;
use Throwable;

/**
 * @covers \LaravelKafka\Exceptions\KafkaException
 * @covers \LaravelKafka\Exceptions\SerializationException
 * @covers \LaravelKafka\Exceptions\DlqException
 */
final class ExceptionTest extends TestCase
{
    public function testAllExtendRuntimeException(): void
    {
        self::assertInstanceOf(RuntimeException::class, new KafkaException('k'));
        self::assertInstanceOf(RuntimeException::class, new SerializationException('s'));
        self::assertInstanceOf(RuntimeException::class, new DlqException('d'));
    }

    public function testAllExtendThrowable(): void
    {
        self::assertInstanceOf(Throwable::class, new KafkaException('k'));
        self::assertInstanceOf(Throwable::class, new SerializationException('s'));
        self::assertInstanceOf(Throwable::class, new DlqException('d'));
    }

    public function testMessageIsPreserved(): void
    {
        $e = new KafkaException('specific failure');
        self::assertSame('specific failure', $e->getMessage());
    }

    public function testChainException(): void
    {
        $previous = new \RuntimeException('root cause');
        $e = new KafkaException('wrapper', 0, $previous);
        self::assertSame($previous, $e->getPrevious());
    }
}
