<?php

declare(strict_types=1);

namespace LaravelKafka\Queue\Failed;

use LaravelKafka\Queue\KafkaJob;
use Throwable;

/**
 * 失败任务处理器接口（v0.1 三模式契约）。
 *
 * ## 角色
 *
 * 定义"消息处理失败后，业务方想怎么收尾"的统一契约。
 * `NativeHandler` 在 `HandlerResult::dlq/fail` 时调用本接口的 `handle()`。
 *
 * ## 三种实现（v0.1）
 *
 * | 实现 | 行为 | 适用场景 |
 * | --- | --- | --- |
 * | {@see DatabaseFailedJobHandler} | 写 `failed_jobs` 表 | Laravel 标准做法，便于人工补跑 |
 * | {@see DlqFailedJobHandler} | 写 DLQ topic | 跨服务 / 自动消费 / 时效性强 |
 * | {@see HybridFailedJobHandler} | 写 DB **同时**写 DLQ | 兜底最全：DB 给人看，DLQ 给机器 |
 *
 * ## 模式选择
 *
 * `config/kafka.php` 的 `failed.mode`：
 * - `database` → `DatabaseFailedJobHandler`
 * - `dlq` → `DlqFailedJobHandler`
 * - `hybrid`（默认）→ `HybridFailedJobHandler`
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 只支持 `failed-job-provider`（Laravel 标准的 `Failed\Provider`），
 * 我们额外提供 DLQ + Hybrid，业务方按业务场景挑。
 */
interface FailedJobHandlerInterface
{
    /**
     * 处理一条失败消息（写 DB / 写 DLQ / 都写）。
     *
     * 实现方**必须**：
     *  - 抛出 `Throwable` 时不吞（让 NativeHandler 知道"连兜底都失败了"，走 requeue）
     *  - 写 DLQ 失败时抛 {@see \LaravelKafka\Exceptions\DlqException}（Hybrid 模式可识别）
     *  - 写 DB 失败时抛 `Throwable`（让消息重试，避免数据丢失）
     *
     * @param KafkaJob $job       失败的 Laravel Job（含原始 payload / headers / topic）
     * @param Throwable $exception 原始异常（业务方记录到 DB / DLQ payload 便于排查）
     * @param FailedContext $context 上下文（topic / partition / offset / 尝试次数 / 时间戳）
     * @return void
     * @throws \Throwable 兜底失败时（DlqException / DatabaseException 等）
     */
    public function handle(KafkaJob $job, Throwable $exception, FailedContext $context): void;
}
