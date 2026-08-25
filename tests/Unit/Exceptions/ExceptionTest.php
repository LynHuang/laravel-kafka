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
        $this->assertInstanceOf(RuntimeException::class, new KafkaException('k'));
        $this->assertInstanceOf(RuntimeException::class, new SerializationException('s'));
        $this->assertInstanceOf(RuntimeException::class, new DlqException('d'));
    }

    public function testAllExtendThrowable(): void
    {
        $this->assertInstanceOf(Throwable::class, new KafkaException('k'));
        $this->assertInstanceOf(Throwable::class, new SerializationException('s'));
        $this->assertInstanceOf(Throwable::class, new DlqException('d'));
    }

    public function testMessageIsPreserved(): void
    {
        $e = new KafkaException('specific failure');
        $this->assertSame('specific failure', $e->getMessage());
    }

    public function testChainException(): void
    {
        $previous = new \RuntimeException('root cause');
        $e = new KafkaException('wrapper', 0, $previous);
        $this->assertSame($previous, $e->getPrevious());
    }
}
