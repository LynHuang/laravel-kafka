<?php

declare(strict_types=1);

namespace LaravelKafka\Queue\Failed;

use Illuminate\Database\ConnectionInterface;
use LaravelKafka\Queue\KafkaJob;
use Ramsey\Uuid\UuidInterface;
use Throwable;

/**
 * 把失败任务写入数据库表（v0.1 三个失败处理实现之一）。
 *
 * ## 表结构
 *
 * 与 Laravel `failed_jobs` 标准 schema 完全一致，**兼容 `php artisan queue:failed` + `queue:retry`**：
 *
 * ```sql
 * CREATE TABLE failed_jobs (
 *   id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
 *   uuid        VARCHAR(36) UNIQUE,
 *   connection  VARCHAR(255),
 *   queue       VARCHAR(255),
 *   payload     LONGTEXT,
 *   exception   LONGTEXT,
 *   failed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
 * );
 * ```
 *
 * ## 业务方使用
 *
 * - `php artisan queue:failed` 列出所有失败任务
 * - `php artisan queue:retry {uuid}` 重投
 * - `php artisan queue:forget {uuid}` 删除
 *
 * 这套命令**全部**沿用 Laravel 内置（业务方零学习成本）。
 *
 * ## payload 格式
 *
 * 写表时**重建** Laravel Job payload 结构（即使 Kafka 原始 payload 不是 Laravel 格式）：
 *
 * ```json
 * {
 *   "uuid": "...", "displayName": "App\\Jobs\\MyJob",
 *   "job": "Illuminate\\Queue\\CallQueuedHandler@call",
 *   "maxTries": null, "data": { "commandName": "...", "command": "..." }
 * }
 * ```
 *
 * 这样 `queue:retry` 能直接拿 payload 反序列化回 Job 实例。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 写表字段多（`failed_at` 用 `Carbon::now()`），
 * 我们用 `date('Y-m-d H:i:s')` 与 Laravel 内置一致。
 */
final class DatabaseFailedJobHandler implements FailedJobHandlerInterface
{
    /**
     * Laravel DB 连接。
     */
    private ConnectionInterface $database;

    /**
     * 目标表名（默认 `failed_jobs`，可配）。
     */
    private string $table;

    /**
     * UUID 工厂（Ramsey UUID v4）。
     */
    private UuidInterface $uuidFactory;

    /**
     * @param ConnectionInterface $database Laravel DB 连接（从 `ConnectionResolverInterface` 拿）
     * @param string $table 目标表名（默认 `failed_jobs`）
     * @param UuidInterface $uuidFactory UUID 工厂（用于写入 `uuid` 字段）
     */
    public function __construct(
        ConnectionInterface $database,
        string $table,
        UuidInterface $uuidFactory
    ) {
        $this->database = $database;
        $this->table = $table;
        $this->uuidFactory = $uuidFactory;
    }

    /**
     * 把失败任务写入 DB。
     *
     * ## 流程
     *
     *  1. 生成 UUID v4
     *  2. `encodePayload()` 重建 Laravel Job payload 结构
     *  3. `encodeException()` 拼装 class / message / trace
     *  4. `INSERT INTO failed_jobs (...)` 一次插入
     *
     * ## 失败处理
     *
     * DB 异常（如 connection lost）会**直接逃逸**出去（不 try/catch），
     * 让 {@see HybridFailedJobHandler} 知道"连 DB 写都失败"，走 DLQ 兜底。
     *
     * @param KafkaJob $job 失败的 Laravel Job
     * @param Throwable $exception 业务抛出的异常
     * @param FailedContext $context 失败上下文（本 handler 不用）
     * @return void
     * @throws \Throwable DB 写入失败时（`QueryException` 等）
     */
    public function handle(KafkaJob $job, Throwable $exception, FailedContext $context): void
    {
        $uuid = (string) $this->uuidFactory->toString();
        $payload = $this->encodePayload($job);
        $exceptionText = $this->encodeException($exception);

        $this->database->table($this->table)->insert([
            'uuid' => $uuid,
            'connection' => $job->getConnectionName(),
            'queue' => $job->getQueue(),
            'payload' => $payload,
            'exception' => $exceptionText,
            'failed_at' => date('Y-m-d H:i:s'),
        ]);
    }

    /**
     * 重建 Laravel Job payload（兼容 `queue:retry` 反序列化）。
     *
     * 关键字段：
     *  - `data.commandName` = Job 类名（`$job->getName()`）
     *  - `data.command` = Kafka 原始 payload（`$job->getRawBody()`）
     *
     * @param KafkaJob $job
     * @return string JSON 字符串
     */
    private function encodePayload(KafkaJob $job): string
    {
        $payload = [
            'uuid' => (string) $job->getJobId(),
            'displayName' => $job->resolveName(),
            'job' => 'Illuminate\\Queue\\CallQueuedHandler@call',
            'maxTries' => null,
            'maxExceptions' => null,
            'backoff' => null,
            'timeout' => null,
            'retryUntil' => null,
            'data' => [
                'commandName' => $job->getName(),
                'command' => $job->getRawBody(),
            ],
        ];
        return json_encode($payload, JSON_UNESCAPED_UNICODE);
    }

    /**
     * 把异常拼成 `class\nmessage\ntrace` 三行格式。
     *
     * 与 Laravel 内置 `Illuminate\Queue\Failed\DatabaseFailedJobProvider` 一致。
     *
     * @param Throwable $exception
     * @return string
     */
    private function encodeException(Throwable $exception): string
    {
        return implode("\n", [
            get_class($exception),
            $exception->getMessage(),
            $exception->getTraceAsString(),
        ]);
    }
}
