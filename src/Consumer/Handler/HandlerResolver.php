<?php

declare(strict_types=1);

namespace LaravelKafka\Consumer\Handler;

use LaravelKafka\Producer\Message;

/**
 * Handler 解析器：决定一条 Kafka 消息该交给哪个 handler 处理（v0.1 简单版）。
 *
 * ## 角色
 *
 * `Consumer::consume()` 拿到一条消息后，调用 `HandlerResolver::resolve($topic, $message)`
 * 拿到具体的 {@see HandlerInterface}，再去调 `handle()`。
 *
 * ## v0.1 限制
 *
 * - 所有消息**统一**走 {@see NativeHandler}（Laravel Job 处理）
 * - v0.3 扩展：根据 `$message->header('x-handler')` 路由到不同 handler
 *
 * ## 为什么 v0.1 不做 per-topic handler
 *
 * 业务方 push 时只能选**一个** Kafka connection，一个 connection 一个 handler。
 * 想 per-topic handler 需在 v0.3 引入 "handler registry"（map topic → handler），不是 v0.1 范围。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 没有 Resolver 概念，handler 在 `kafka:work` 命令里直接 `new NativeHandler`；
 * 我们用 Resolver 抽象 → v0.3 升级时业务方代码不用改。
 */
final class HandlerResolver
{
    /**
     * Laravel Job 处理器（v0.1 唯一实现）。
     */
    private NativeHandler $nativeHandler;

    /**
     * @param NativeHandler $nativeHandler 注入的 NativeHandler 实例
     */
    public function __construct(NativeHandler $nativeHandler)
    {
        $this->nativeHandler = $nativeHandler;
    }

    /**
     * 决定一条消息由哪个 handler 处理。
     *
     * v0.1 一律返回 `NativeHandler`。
     * v0.3+ 计划：根据 `$message->header('x-handler')` 路由。
     *
     * @param string $topic 消息来源 topic
     * @param Message $message 消息值对象
     * @return HandlerInterface
     */
    public function resolve(string $topic, Message $message): HandlerInterface
    {
        // v0.1：所有消息都按 Laravel Job 处理
        // v0.3：根据 $message->header('x-handler') 路由到不同 handler
        return $this->nativeHandler;
    }
}
