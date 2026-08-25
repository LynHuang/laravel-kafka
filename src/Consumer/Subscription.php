<?php

declare(strict_types=1);

namespace LaravelKafka\Consumer;

/**
 * 订阅描述：worker 要消费的 topic 集合（v0.1 值对象）。
 *
 * ## 角色
 *
 * `kafka:work` 命令启动 worker 时，把 `config/kafka.php` 的 `topics[]` 列表
 * 打包成 `Subscription`，传给 {@see Consumer} 的 `subscribe()`。
 * 内部是 `array<int, string>` + 唯一化 + 非空校验，避免业务方传 `[]` 进去。
 *
 * ## v0.1 限制
 *
 * - 单 worker 一组 topic，全走**同一个** {@see \LaravelKafka\Consumer\Handler\HandlerInterface}
 * - v0.3 扩展：per-topic handler（用 `array<string, HandlerInterface>`）
 *
 * ## 不可变
 *
 * `final` + 构造时一次性写 `$topics`，**不**提供 `addTopic()` 等 mutation 方法。
 * 业务方如果想动态订阅，只能 `new Subscription($newArray)` 重建。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 用字符串 "topic1,topic2" + 解析，复杂场景下不灵活；我们用 array + 值对象。
 */
final class Subscription
{
    /**
     * 已去重的 topic 名列表。
     *
     * 构造时 `array_values(array_unique(array_map('strval', $topics)))`：
     *  - 去空（业务方传 `''` 时不抛，但会被保留）
     *  - 去重
     *  - 强制 string（兼容业务方传 int）
     *
     * @var array<int, string>
     */
    private $topics;

    /**
     * @param array<int, string> $topics 要订阅的 topic 列表（业务方传 `['emails', 'orders']`）
     * @throws \InvalidArgumentException 空数组时（worker 必须订阅至少一个 topic）
     */
    public function __construct(array $topics)
    {
        if (count($topics) === 0) {
            throw new \InvalidArgumentException('Subscription requires at least one topic.');
        }
        $this->topics = array_values(array_unique(array_map('strval', $topics)));
    }

    /**
     * 拿到所有订阅的 topic 列表。
     *
     * @return array<int, string>
     */
    public function topics(): array
    {
        return $this->topics;
    }

    /**
     * 拿到第一个 topic（兼容 v0.1 单 topic 场景）。
     *
     * v0.1 的 `kafka:work` 实际把多 topic 都过同一个 handler，但有些场景（如健康检查）
     * 只需要拿一个代表性 topic，就用 `firstTopic()`。
     *
     * @return string 第一个 topic
     * @throws \RuntimeException topics 为空时（构造时已保证非空，业务方不应该触发）
     */
    public function firstTopic(): string
    {
        return $this->topics[0];
    }
}
