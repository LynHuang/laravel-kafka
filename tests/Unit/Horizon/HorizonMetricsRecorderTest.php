<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Horizon;

use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Horizon\HorizonMetricsRecorder;
use LaravelKafka\Tests\TestCase;
use Mockery as m;

/**
 * v0.4: HorizonMetricsRecorder 单元测试。
 *
 * @covers \LaravelKafka\Horizon\HorizonMetricsRecorder
 */
final class HorizonMetricsRecorderTest extends TestCase
{
    public function testPrefixAndConnectionGetters(): void
    {
        $redis = m::mock();
        $recorder = new HorizonMetricsRecorder($redis, 'my-conn', 'my-prefix:');

        self::assertSame('my-prefix:', $recorder->prefix());
        self::assertSame('my-conn', $recorder->connection());
    }

    public function testDefaultPrefixIsHorizon(): void
    {
        $redis = m::mock();
        $recorder = new HorizonMetricsRecorder($redis);

        self::assertSame('horizon:', $recorder->prefix());
        self::assertSame('horizon', $recorder->connection());
    }

    public function testConstructorRejectsNullRedis(): void
    {
        $this->expectException(KafkaException::class);
        $this->expectExceptionMessage('cannot be null');

        new HorizonMetricsRecorder(null);
    }

    public function testIncrementQueueCallsEvalWithHorizonLua(): void
    {
        $redis = m::mock();
        $conn = m::mock();

        $redis->shouldReceive('connection')
            ->with('horizon')
            ->once()
            ->andReturn($conn);

        // 期望 eval 被调，参数 = [LUA, 2, 'horizon:queue:emails', 'horizon:measured_queues', '12.5']
        $conn->shouldReceive('eval')
            ->once()
            ->with(
                m::on(function ($lua) {
                    // 验证 Lua 包含 Horizon 5.x 关键行
                    return strpos($lua, "redis.call('hsetnx'") !== false
                        && strpos($lua, "redis.call('sadd'") !== false
                        && strpos($lua, "redis.call('hmset'") !== false;
                }),
                2,
                'horizon:queue:emails',
                'horizon:measured_queues',
                '12.5'  // runtime 字符串
            )
            ->andReturnNull();

        $recorder = new HorizonMetricsRecorder($redis);
        $recorder->incrementQueue('emails', 12.5);
    }

    public function testIncrementJobCallsEvalWithJobClassKey(): void
    {
        $redis = m::mock();
        $conn = m::mock();

        $redis->shouldReceive('connection')->andReturn($conn);

        $conn->shouldReceive('eval')
            ->once()
            ->with(
                m::any(),
                2,
                'horizon:job:App\\Jobs\\SendEmail',
                'horizon:measured_jobs',
                '100.0'
            )
            ->andReturnNull();

        $recorder = new HorizonMetricsRecorder($redis);
        $recorder->incrementJob('App\\Jobs\\SendEmail', 100.0);
    }

    public function testCustomPrefixIsApplied(): void
    {
        $redis = m::mock();
        $conn = m::mock();

        $redis->shouldReceive('connection')->andReturn($conn);

        $conn->shouldReceive('eval')
            ->once()
            ->with(
                m::any(),
                2,
                'myapp:queue:reports',
                'myapp:measured_queues',
                '0.5'
            )
            ->andReturnNull();

        $recorder = new HorizonMetricsRecorder($redis, 'horizon', 'myapp:');
        $recorder->incrementQueue('reports', 0.5);
    }

    public function testRuntimeWithCommaIsConvertedToDot(): void
    {
        $redis = m::mock();
        $conn = m::mock();

        $redis->shouldReceive('connection')->andReturn($conn);

        // Lua 脚本不接受 ',' 作为小数点（PHP 8.0+ deprecated）→ 必须替换
        $conn->shouldReceive('eval')
            ->once()
            ->with(
                m::any(),
                2,
                'horizon:queue:test',
                'horizon:measured_queues',
                '12.5'  // '12,5' → '12.5'
            )
            ->andReturnNull();

        $recorder = new HorizonMetricsRecorder($redis);
        $recorder->incrementQueue('test', 12.5);
    }
}
