<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Consumer;

use LaravelKafka\Consumer\Consumer;
use LaravelKafka\Consumer\Subscription;
use LaravelKafka\Tests\TestCase;
use Mockery as m;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Message as RdKafkaMessage;

/**
 * v0.3 Step 1: 批量消费 (pollBatch + commitBatch) 单元测试。
 *
 * 覆盖：
 *  - pollBatch 返回最多 max 条消息
 *  - pollBatch 在拉到 1 条后遇 TIMED_OUT 早返回
 *  - pollBatch 在全部 TIMED_OUT 时返回 []
 *  - pollBatch 抛 InvalidArgumentException 当 max <= 0
 *  - commitBatch 调 commitAsync 一次
 *
 * @covers \LaravelKafka\Consumer\Consumer
 */
final class ConsumerBatchTest extends TestCase
{
    public function testPollBatchReturnsUpToMaxMessages(): void
    {
        $rdConsumer = m::mock(RdKafkaConsumer::class);
        $rdConsumer->shouldReceive('subscribe')->with(['test'])->once();

        // 模拟 3 条 OK 消息：拉够 max=3 后在 for 循环开头 break，不会调第 4 次
        $rdConsumer->shouldReceive('consume')
            ->times(3)
            ->andReturn(
                $this->makeMsg(RD_KAFKA_RESP_ERR_NO_ERROR, 'msg-1'),
                $this->makeMsg(RD_KAFKA_RESP_ERR_NO_ERROR, 'msg-2'),
                $this->makeMsg(RD_KAFKA_RESP_ERR_NO_ERROR, 'msg-3')
            );

        $consumer = new Consumer($rdConsumer, new Subscription(['test']));
        $messages = $consumer->pollBatch(3, 5000);

        $this->assertCount(3, $messages);
        $this->assertSame('msg-1', $messages[0]->payload());
        $this->assertSame('msg-2', $messages[1]->payload());
        $this->assertSame('msg-3', $messages[2]->payload());
    }

    public function testPollBatchEarlyReturnOnTimedOutAfterFirstMessage(): void
    {
        $rdConsumer = m::mock(RdKafkaConsumer::class);
        $rdConsumer->shouldReceive('subscribe')->with(['test'])->once();

        // 1 条 OK + 1 个 TIMED_OUT → 早返回（不空等到 deadline）
        $rdConsumer->shouldReceive('consume')
            ->times(2)
            ->andReturn(
                $this->makeMsg(RD_KAFKA_RESP_ERR_NO_ERROR, 'first-msg'),
                $this->makeMsg(RD_KAFKA_RESP_ERR__TIMED_OUT, '')
            );

        $consumer = new Consumer($rdConsumer, new Subscription(['test']));
        $messages = $consumer->pollBatch(50, 5000);

        $this->assertCount(1, $messages);
        $this->assertSame('first-msg', $messages[0]->payload());
    }

    public function testPollBatchReturnsEmptyOnAllTimeout(): void
    {
        $rdConsumer = m::mock(RdKafkaConsumer::class);
        $rdConsumer->shouldReceive('subscribe')->with(['test'])->once();

        // 全是 TIMED_OUT → 连续 2 次空 poll 后退出 → 调用 2 次
        $rdConsumer->shouldReceive('consume')
            ->times(2)
            ->andReturn(
                $this->makeMsg(RD_KAFKA_RESP_ERR__TIMED_OUT, ''),
                $this->makeMsg(RD_KAFKA_RESP_ERR__TIMED_OUT, '')
            );

        $consumer = new Consumer($rdConsumer, new Subscription(['test']));
        $messages = $consumer->pollBatch(10, 5000);

        $this->assertSame([], $messages);
    }

    public function testPollBatchStopsAfterMaxReached(): void
    {
        $rdConsumer = m::mock(RdKafkaConsumer::class);
        $rdConsumer->shouldReceive('subscribe')->with(['test'])->once();

        // mock 5 次 consume，但 max=2 → 只调 2 次（拉够 2 条就退出）
        $rdConsumer->shouldReceive('consume')
            ->times(2)
            ->andReturn(
                $this->makeMsg(RD_KAFKA_RESP_ERR_NO_ERROR, 'msg-1'),
                $this->makeMsg(RD_KAFKA_RESP_ERR_NO_ERROR, 'msg-2')
            );

        $consumer = new Consumer($rdConsumer, new Subscription(['test']));
        $messages = $consumer->pollBatch(2, 5000);

        $this->assertCount(2, $messages);
    }

    public function testPollBatchThrowsOnInvalidMax(): void
    {
        $rdConsumer = m::mock(RdKafkaConsumer::class);
        $rdConsumer->shouldReceive('subscribe')->with(['test'])->once();

        $consumer = new Consumer($rdConsumer, new Subscription(['test']));

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('pollBatch max must be > 0, got 0');

        $consumer->pollBatch(0, 1000);
    }

    public function testPollBatchThrowsOnNegativeMax(): void
    {
        $rdConsumer = m::mock(RdKafkaConsumer::class);
        $rdConsumer->shouldReceive('subscribe')->with(['test'])->once();

        $consumer = new Consumer($rdConsumer, new Subscription(['test']));

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('pollBatch max must be > 0, got -5');

        $consumer->pollBatch(-5, 1000);
    }

    public function testCommitBatchCallsCommitAsync(): void
    {
        $rdConsumer = m::mock(RdKafkaConsumer::class);
        $rdConsumer->shouldReceive('subscribe')->with(['test'])->once();

        $rdConsumer->shouldReceive('commitAsync')->once();

        $consumer = new Consumer($rdConsumer, new Subscription(['test']));
        $consumer->commitBatch();

        // Mockery 自动验证 shouldReceive('commitAsync')->once()
        $this->assertTrue(true);
    }

    /**
     * 构造测试用 RdKafka\Message。
     */
    private function makeMsg(int $err, string $payload): RdKafkaMessage
    {
        $msg = new RdKafkaMessage();
        $msg->err = $err;
        $msg->payload = $payload;
        $msg->topic_name = 'test';
        $msg->partition = 0;
        $msg->offset = 0;
        $msg->key = null;
        $msg->timestamp = 0;
        $msg->headers = [];
        return $msg;
    }
}
