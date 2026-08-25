<?php

declare(strict_types=1);

namespace LaravelKafka\Support\Testing;

use LaravelKafka\Producer\Message;

/**
 * Fake 模式下记录所有 "published" 消息的存储。
 *
 * ## 设计动机
 *
 * v0.1 没有 Fake 能力，业务方只能 `Bus::fake()` 验证 Job 被 dispatch，**不能**验证
 * "消息发到了 topic X、带了 header Y、payload 是 Z"。
 *
 * KafkaFake 的所有断言方法都基于本存储的内容。fake 模式由 `Kafka::fake()` 触发，
 * `KafkaQueue::pushRaw` 在 fake 分支调用本类 `record()`。
 *
 * ## 生命周期
 *
 * - `Kafka::fake()` 时通过 Laravel 容器 `instance()` 单例化
 * - 整个测试期间共享这一个实例（业务方多次 fake 不会冲突）
 * - 测试结束随容器销毁自动释放
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges Fake 内部只存 `ProducerMessage` 数组，丢失 topic 信息。
 * 我们存 `['topic' => string, 'message' => Producer\Message]` 二元组，断言时既能
 * 验证 topic，也能验证 payload / key / header。
 *
 * @see \LaravelKafka\Support\Testing\KafkaFake
 * @see \LaravelKafka\Manager\KafkaManager::fake()
 */
final class FakeMessageStorage
{
    /**
     * 已记录的 "已发布" 消息列表。
     *
     * 每条形如 `['topic' => string, 'message' => Producer\Message]`：
     * - `topic`：业务方传入的逻辑队列经 `KafkaConfig::resolveTopic()` 解析后的物理 topic 名
     * - `message`：构造好的 `Producer\Message` 值对象（含 payload / headers / key）
     *
     * @var array<int, array{topic: string, message: Message}>
     */
    private array $published = [];

    /**
     * 记录一次 push 调用（fake 路径入口）。
     *
     * 由 `KafkaQueue::pushRaw` 在 fake 分支调用。
     * 真发路径**不**调本方法。
     *
     * @param string $topic  物理 topic 名（已解析过）
     * @param Message $message 构造好的消息值对象
     * @return void
     */
    public function record(string $topic, Message $message): void
    {
        $this->published[] = [
            'topic' => $topic,
            'message' => $message,
        ];
    }

    /**
     * 拿到所有记录的副本（防止外部 mutate 内部数组）。
     *
     * @return array<int, array{topic: string, message: Message}>
     */
    public function all(): array
    {
        return $this->published;
    }

    /**
     * 已记录消息数（便利方法，等价于 `count($this->all())`）。
     *
     * @return int
     */
    public function count(): int
    {
        return count($this->published);
    }

    /**
     * 是否为空（业务方 `assertNothingPushed` 前检查）。
     *
     * @return bool
     */
    public function isEmpty(): bool
    {
        return $this->published === [];
    }

    /**
     * 清空记录（业务方在测试 setUp 调用，避免上一次测试污染）。
     *
     * 不必每次调——`Kafka::fake()` 会创建新 storage（容器 instance()）。
     * 但如果业务方在测试中途想"重置到无消息状态"，可调此方法。
     *
     * @return void
     */
    public function flush(): void
    {
        $this->published = [];
    }
}
