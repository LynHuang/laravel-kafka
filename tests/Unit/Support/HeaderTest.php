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
        $this->assertSame('x-trace-id', Header::TRACE_ID);
        $this->assertSame('x-queue', Header::QUEUE);
        $this->assertSame('x-connection', Header::CONNECTION);
        $this->assertSame('x-enqueued-at', Header::ENQUEUED_AT);
        $this->assertSame('x-attempt', Header::RETRY_COUNT);
        $this->assertSame('x-serializer', Header::SERIALIZER);
        $this->assertSame('x-available-at', Header::AVAILABLE_AT);
        $this->assertSame('x-job-id', Header::JOB_ID);
        $this->assertSame('x-original-topic', Header::ORIGINAL_TOPIC);
        $this->assertSame('x-original-partition', Header::ORIGINAL_PARTITION);
        $this->assertSame('x-original-offset', Header::ORIGINAL_OFFSET);
        $this->assertSame('x-original-headers', Header::ORIGINAL_HEADERS);
        $this->assertSame('x-failed-at', Header::FAILED_AT);
        $this->assertSame('x-exception-class', Header::EXCEPTION_CLASS);
        $this->assertSame('x-exception-message', Header::EXCEPTION_MESSAGE);
        $this->assertSame('x-exception-trace', Header::EXCEPTION_TRACE);
    }

    public function testCannotBeInstantiated(): void
    {
        $reflection = new ReflectionClass(Header::class);
        $this->assertTrue($reflection->isFinal());
        $constructor = $reflection->getConstructor();
        $this->assertNotNull($constructor);
        $this->assertTrue($constructor->isPrivate());
    }
}
