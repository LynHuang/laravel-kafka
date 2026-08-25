<?php

declare(strict_types=1);

namespace LaravelKafka\Consumer\Handler;

use LaravelKafka\Producer\Message;

/**
 * 消息处理器接口（v0.1）。
 *
 * ## 角色
 *
 * 业务方实现本接口，定义"拿到一条 Kafka 消息后干什么"。
 * 典型实现 {@see NativeHandler} 负责：
 *  - 读 header `x-serializer` 选反序列化器
 *  - 从 payload 还原 Laravel Job（`unserialize($payload)`）
 *  - `Job::fire()` 执行业务
 *  - 返回 {@see HandlerResult} 决定后续动作（ack / requeue / dlq）
 *
 * ## 业务方自定义
 *
 * ```php
 * class MyHandler implements HandlerInterface {
 *     public function handle(Message $message): HandlerResult {
 *         // 拿到 topic / partition / key
 *         $topic = $message->topic();
 *         $payload = unserialize($message->payload());
 *         try {
 *             MyJob::dispatch($payload)->handle();
 *             return HandlerResult::ack();
 *         } catch (Throwable $e) {
 *             return HandlerResult::dlq($e);
 *         }
 *     }
 * }
 * ```
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 把 handler 直接内嵌到 Consumer 构造器 lambda，
 * 我们用接口（更易 mock + 测试 + 业务方解耦）。
 */
interface HandlerInterface
{
    /**
     * 处理一条 Kafka 消息。
     *
     * 实现方必须：
     *  1. **不**自己 catch 致命异常后吞掉 —— 把异常放到 `HandlerResult::fail($e)` 里
     *  2. **不**做阻塞 IO（如同步 HTTP 调用），否则 `kafka:work` 长驻进程会卡住
     *  3. **幂等**：Kafka at-least-once 语义，broker 重启会重投
     *
     * @param Message $message 来自 librdkafka 的消息（含 payload / headers / key / topic）
     * @return HandlerResult 决定后续动作（ack / requeue / dlq / fail）
     */
    public function handle(Message $message): HandlerResult;
}
