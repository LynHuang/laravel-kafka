<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Support;

use LaravelKafka\Support\Header;
use LaravelKafka\Tests\TestCase;
use ReflectionClass;

/**
 * @covers \LaravelKafka\Support\Header
 */
final class HeaderTest extends TestCase
{
    public function testConstantsAreExposed(): void
    {
        self::assertSame('x-trace-id', Header::TRACE_ID);
        self::assertSame('x-queue', Header::QUEUE);
        self::assertSame('x-connection', Header::CONNECTION);
        self::assertSame('x-enqueued-at', Header::ENQUEUED_AT);
        self::assertSame('x-attempt', Header::RETRY_COUNT);
        self::assertSame('x-serializer', Header::SERIALIZER);
        self::assertSame('x-available-at', Header::AVAILABLE_AT);
        self::assertSame('x-job-id', Header::JOB_ID);
        self::assertSame('x-original-topic', Header::ORIGINAL_TOPIC);
        self::assertSame('x-original-partition', Header::ORIGINAL_PARTITION);
        self::assertSame('x-original-offset', Header::ORIGINAL_OFFSET);
        self::assertSame('x-original-headers', Header::ORIGINAL_HEADERS);
        self::assertSame('x-failed-at', Header::FAILED_AT);
        self::assertSame('x-exception-class', Header::EXCEPTION_CLASS);
        self::assertSame('x-exception-message', Header::EXCEPTION_MESSAGE);
        self::assertSame('x-exception-trace', Header::EXCEPTION_TRACE);
    }

    public function testCannotBeInstantiated(): void
    {
        $reflection = new ReflectionClass(Header::class);
        self::assertTrue($reflection->isFinal());
        $constructor = $reflection->getConstructor();
        self::assertNotNull($constructor);
        self::assertTrue($constructor->isPrivate());
    }
}
