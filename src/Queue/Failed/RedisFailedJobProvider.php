<?php

declare(strict_types=1);

namespace LaravelKafka\Queue\Failed;

use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Support\Facades\Redis;
use Ramsey\Uuid\Uuid;

/**
 * Redis-based FailedJobProvider（v0.4.5 新增）。
 *
 * 实现 Laravel 标准 {@see FailedJobProviderInterface} 6 个方法，
 * 让 `php artisan queue:failed` / `queue:retry` / `queue:forget` / `queue:flush`
 * 在**没有 pdo_sqlite / MySQL** 的业务环境也能 work。
 *
 * ## 存储结构
 *
 * | Redis Key | 类型 | 用途 |
 * | --- | --- | --- |
 * | `{listKey}` (默认 `kafka:failed_jobs`) | sorted set | 按时间倒序排，score = failed_at ms |
 * | `{hashPrefix}{uuid}` (默认 `kafka:failed_job:`) | string (JSON) | 单条 failed job payload + exception |
 *
 * ## 容量管理
 *
 * `maxItems` 默认 1000，超出后 zremrangebyrank 删最老的。防止 Redis 无限增长。
 *
 * ## 与业务方业务方环境的适配
 *
 * 业务方业务方 Horizon 已经用 Redis（127.0.0.1:6379），本 provider 走 `default` 连接
 * 就能复用——不需要单独建连接。
 *
 * ## 业务方使用
 *
 * ```php
 * // config/queue.php
 * 'failed' => [
 *     'driver' => 'kafka-redis',  // 触发本 provider
 *     'connection' => 'default',  // Redis 连接名
 *     'key' => 'kafka:failed_jobs',  // list key
 * ],
 * ```
 */
final class RedisFailedJobProvider implements FailedJobProviderInterface
{
    /**
     * Redis 连接名（对应 config/database.php redis.connections.{name}）。
     */
    private string $connection;

    /**
     * Sorted set key，存所有 failed job 的 uuid 列表。
     */
    private string $listKey;

    /**
     * 单条 failed job 的存储 key 前缀。
     */
    private string $hashPrefix;

    /**
     * 最大保留条数（超出后 zremrangebyrank 删最老的）。
     */
    private int $maxItems;

    public function __construct(
        string $connection = 'default',
        string $listKey = 'kafka:failed_jobs',
        string $hashPrefix = 'kafka:failed_job:',
        int $maxItems = 1000
    ) {
        $this->connection = $connection;
        $this->listKey = $listKey;
        $this->hashPrefix = $hashPrefix;
        $this->maxItems = max(1, $maxItems);
    }

    /**
     * 记录一条 failed job（Laravel `queue:failed --id={uuid}` 拿这个）。
     *
     * @param string $connection connection 名
     * @param string $queue 队列名
     * @param string $payload Laravel Job payload (PHP serialize 字符串)
     * @param \Throwable $exception 原始异常
     * @return string|null failed job uuid（`queue:retry` 用这个 id）
     */
    public function log($connection, $queue, $payload, $exception)
    {
        $uuid = (string) Uuid::uuid4();
        $nowMs = (int) (microtime(true) * 1000);

        $data = [
            'id' => $uuid,
            'connection' => (string) $connection,
            'queue' => (string) $queue,
            'payload' => (string) $payload,
            'exception' => (string) $exception,
            'failed_at' => date('Y-m-d H:i:s', (int) ($nowMs / 1000)),
            'timestamp' => $nowMs,
        ];

        $json = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($json === false) {
            return null;
        }

        $this->redis()->zadd($this->listKey, $nowMs, $uuid);
        $this->redis()->set($this->hashPrefix . $uuid, $json);

        // 容量管理：超过 maxItems 删最老的
        $count = (int) $this->redis()->zcard($this->listKey);
        if ($count > $this->maxItems) {
            $removeCount = $count - $this->maxItems;
            $oldUuids = (array) $this->redis()->zrange($this->listKey, 0, $removeCount - 1);
            if (! empty($oldUuids)) {
                $this->redis()->zremrangebyrank($this->listKey, 0, $removeCount - 1);
                foreach ($oldUuids as $oldUuid) {
                    $this->redis()->del($this->hashPrefix . $oldUuid);
                }
            }
        }

        return $uuid;
    }

    /**
     * 列出所有 failed jobs（按 failed_at 倒序）。
     *
     * @return array<int, array<string, mixed>>
     */
    public function all()
    {
        $uuids = (array) $this->redis()->zrevrange($this->listKey, 0, -1);
        return $this->loadMany($uuids);
    }

    /**
     * 按 uuid 找单条 failed job。
     *
     * @param string|int $id
     * @return array<string, mixed>|null
     */
    public function find($id)
    {
        $id = (string) $id;
        $raw = $this->redis()->get($this->hashPrefix . $id);
        if ($raw === null || $raw === false) {
            return null;
        }
        $data = json_decode((string) $raw, true);
        return is_array($data) ? $data : null;
    }

    /**
     * 删一条 failed job（`queue:forget` 命令调用）。
     *
     * @param string|int $id
     * @return bool
     */
    public function forget($id)
    {
        $id = (string) $id;
        $this->redis()->zrem($this->listKey, $id);
        return (int) $this->redis()->del($this->hashPrefix . $id) > 0;
    }

    /**
     * 清空所有 failed jobs（`queue:flush` 命令调用）。
     *
     * @return void
     */
    public function flush()
    {
        $uuids = (array) $this->redis()->zrange($this->listKey, 0, -1);
        if (! empty($uuids)) {
            foreach ($uuids as $uuid) {
                $this->redis()->del($this->hashPrefix . $uuid);
            }
        }
        $this->redis()->del($this->listKey);
    }

    /**
     * 计数（`queue:failed --connection=kafka --queue=default` 显示）。
     *
     * @param string $connection
     * @param string $queue
     * @return int
     */
    public function count($connection, $queue)
    {
        $all = $this->all();
        $count = 0;
        foreach ($all as $job) {
            if (($job['connection'] ?? null) === $connection
                && ($job['queue'] ?? null) === $queue) {
                $count++;
            }
        }
        return $count;
    }

    /**
     * 拿 Redis 连接（lazy, 走 Laravel 容器 facade, 让 connection 改动可热生效）。
     */
    private function redis()
    {
        return Redis::connection($this->connection);
    }

    /**
     * 批量 load 多个 uuid 的 payload。
     *
     * @param array<int, string> $uuids
     * @return array<int, array<string, mixed>>
     */
    private function loadMany(array $uuids): array
    {
        $result = [];
        foreach ($uuids as $uuid) {
            $data = $this->find($uuid);
            if ($data !== null) {
                $result[] = $data;
            }
        }
        return $result;
    }
}
