<?php

declare(strict_types=1);

namespace LaravelKafka\Queue\Failed;

use LaravelKafka\Queue\KafkaJob;
use Throwable;

/**
 * Hybrid 失败处理器（v0.1 默认失败处理实现）。
 *
 * ## 角色
 *
 * 同时持有 {@see DatabaseFailedJobHandler} + {@see DlqFailedJobHandler}，
 * 按业务方配置的决策树分配失败处理。
 *
 * ## 决策树
 *
 * ```
 * if (isFatal($exception) || $attemptNumber >= $maxAttempts) {
 *     // 写 DB + DLQ（双写）
 * } else {
 *     // 仅写 DB（不写 DLQ，给重试一次机会）
 * }
 * ```
 *
 * ## 为什么不是"双写所有失败"
 *
 * - 失败次数 < `max_attempts` 时，写 DLQ 会**污染** DLQ topic（业务方真正想看的是
 *   "再也救不回来"的消息）
 * - 仅写 DB 能保留失败明细给业务方排查，但不进 DLQ 队列
 * - 到 `max_attempts` / 致命异常时双写：DB 给**人**看，DLQ 给**机器**消费
 *
 * ## 双写隔离
 *
 * DB 写失败**不**阻止 DLQ 写（反之亦然）。
 * 都失败时抛 `RuntimeException`（带 DLQ 的 exception 作为 previous）。
 *
 * ## v0.1 限制
 *
 * "isFatal" 走**类名匹配**（`$exception instanceof $class`），
 * v0.3 评估：支持正则 / 异常 message 匹配。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges **只**支持 database 模式；我们 hybrid 兜底更全。
 */
final class HybridFailedJobHandler implements FailedJobHandlerInterface
{
    private DatabaseFailedJobHandler $database;

    private DlqFailedJobHandler $dlq;

    private int $maxAttempts;

    /**
     * 致命异常类名列表（命中立即 DLQ）。
     *
     * @var array<int,string>
     */
    private array $fatalExceptions;

    private int $traceTruncateBytes;

    private int $messageTruncateBytes;

    /**
     * @param DatabaseFailedJobHandler $database
     * @param DlqFailedJobHandler $dlq
     * @param int $maxAttempts 重试上限（业务方在 `kafka.php` 配置）
     * @param array<int,string> $fatalExceptions 致命异常类名列表
     * @param int $traceTruncateBytes 异常 trace 截断字节数（透传给 DLQ handler）
     * @param int $messageTruncateBytes 异常 message 截断字节数（透传给 DLQ handler）
     */
    public function __construct(
        DatabaseFailedJobHandler $database,
        DlqFailedJobHandler $dlq,
        int $maxAttempts,
        array $fatalExceptions,
        int $traceTruncateBytes,
        int $messageTruncateBytes
    ) {
        $this->database = $database;
        $this->dlq = $dlq;
        $this->maxAttempts = $maxAttempts;
        $this->fatalExceptions = $fatalExceptions;
        $this->traceTruncateBytes = $traceTruncateBytes;
        $this->messageTruncateBytes = $messageTruncateBytes;
    }

    /**
     * 处理一条失败消息。
     *
     * ## 流程
     *
     *  1. `$isFatal = isFatal($exception)` —— 致命异常判断
     *  2. `$attemptNumber = $context->attempts() + 1` —— 1-indexed 当前尝试次数
     *  3. `$overLimit = $attemptNumber >= $maxAttempts` —— 已重试满
     *  4. **isFatal || overLimit**：双写（DB + DLQ），任一失败隔离
     *  5. **否则**：仅写 DB（给重试一次机会）
     *
     * ## 失败传播
     *
     * - DB 失败 + DLQ 成功：`error_log` 警告，DLQ 是真理之源
     * - DB 成功 + DLQ 失败：抛 DLQ 异常（让 NativeHandler 走 requeue）
     * - DB 失败 + DLQ 失败：抛 `RuntimeException`（带 DLQ exception 作为 previous）
     *
     * @param KafkaJob $job
     * @param Throwable $exception
     * @param FailedContext $context
     * @return void
     * @throws \Throwable 兜底失败时
     */
    public function handle(KafkaJob $job, Throwable $exception, FailedContext $context): void
    {
        $isFatal = $this->isFatal($exception);
        $attemptNumber = $context->attempts() + 1; // 1-indexed
        $overLimit = $attemptNumber >= $this->maxAttempts;

        if ($isFatal || $overLimit) {
            // 双写：写表 + DLQ
            // 任一失败不应阻止另一个 —— 用 try/catch 隔开
            $dbError = null;
            try {
                $this->database->handle($job, $exception, $context);
            } catch (Throwable $e) {
                $dbError = $e;
            }
            try {
                $this->dlq->handle($job, $exception, $context);
            } catch (Throwable $e) {
                if ($dbError !== null) {
                    throw new \RuntimeException(
                        'Hybrid handler: both database and DLQ writes failed',
                        0,
                        $e
                    );
                }
                throw $e;
            }
            if ($dbError !== null) {
                // DLQ 成功但 database 失败：log 一下，DLQ 才是真理之源
                error_log('[laravel-kafka] hybrid handler: database write failed but DLQ succeeded: ' . $dbError->getMessage());
            }
            return;
        }

        // 未到 max_attempts 且非致命：仅写 database（不写 DLQ）
        // 这是 hybrid 的"宽容"路径——给重试一次机会，但保留失败明细供排查
        $this->database->handle($job, $exception, $context);
    }

    /**
     * 致命异常判断（`$exception instanceof $class`）。
     *
     * ## 匹配规则
     *
     * 类名列表（如 `['App\\Exceptions\\BizFatalException', 'InvalidArgumentException']`）。
     * 子类也算命中（`instanceof` 是 is-a 关系）。
     *
     * @param Throwable $e
     * @return bool true = 致命（直接 DLQ，不等 max_attempts）
     */
    private function isFatal(Throwable $e): bool
    {
        foreach ($this->fatalExceptions as $class) {
            if ($e instanceof $class) {
                return true;
            }
        }
        return false;
    }
}
