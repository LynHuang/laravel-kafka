<?php

declare(strict_types=1);

namespace LaravelKafka\Queue\Failed;

use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Redis\Connections\Connection;
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

        // v0.4.8: Laravel 8 Connection 基类的 zadd/set/zcard 等方法都是通过 __call
        // 转发到 phpredis/predis client, phpstan 静态分析看不到 magic method.
        // 加 @phpstan-ignore-next-line 明确告诉分析器这是 dynamic dispatch.
        /** @var \Illuminate\Redis\Connections\Connection $conn */
        $conn = $this->redis();
        $conn->zadd($this->listKey, $nowMs, $uuid);
        $conn->set($this->hashPrefix . $uuid, $json);

        // 容量管理：超过 maxItems 删最老的
        $count = (int) $conn->zcard($this->listKey);
        if ($count > $this->maxItems) {
            $removeCount = $count - $this->maxItems;
            $oldUuids = (array) $conn->zrange($this->listKey, 0, $removeCount - 1);
            if (! empty($oldUuids)) {
                $conn->zremrangebyrank($this->listKey, 0, $removeCount - 1);
                foreach ($oldUuids as $oldUuid) {
                    $conn->del($this->hashPrefix . $oldUuid);
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
        /** @var \Illuminate\Redis\Connections\Connection $conn */
        $conn = $this->redis();
        $uuids = (array) $conn->zrevrange($this->listKey, 0, -1);
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
        /** @var \Illuminate\Redis\Connections\Connection $conn */
        $conn = $this->redis();
        $raw = $conn->get($this->hashPrefix . $id);
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
        /** @var \Illuminate\Redis\Connections\Connection $conn */
        $conn = $this->redis();
        $conn->zrem($this->listKey, $id);
        return (int) $conn->del($this->hashPrefix . $id) > 0;
    }

    /**
     * 清空 failed jobs（`queue:flush` / `queue:prune-failed` 命令调用）。
     *
     * ## $hours 语义（v0.4.9 兼容 Laravel 8/9/10/11 接口差异）
     *
     * - Laravel 8 接口：`flush()` 无参数 → $hours=null → **全部清空**
     * - Laravel 9/10/11 接口：`flush($hours)` → $hours 非 null → 只删超过 $hours 小时前的
     * - `queue:flush` 命令：调 `flush()` 无参数 = 全清
     * - `queue:prune-failed --hours=N` 命令：调 `flush($hours)` = 只删 N 小时前
     *
     * @param int|null $hours 保留小时数（null = 全部清空）
     * @return void
     */
    public function flush($hours = null)
    {
        /** @var \Illuminate\Redis\Connections\Connection $conn */
        $conn = $this->redis();
        $uuids = (array) $conn->zrange($this->listKey, 0, -1);

        $cutoffMs = null;
        if ($hours !== null) {
            $cutoffMs = (int) ((time() - ((int) $hours * 3600)) * 1000);
        }

        foreach ($uuids as $uuid) {
            $shouldDelete = true;
            if ($cutoffMs !== null) {
                $data = $this->find($uuid);
                $failedAtMs = (int) ($data['timestamp'] ?? 0);
                // 只删超过 cutoff 时间点的 (旧的)
                $shouldDelete = $failedAtMs <= $cutoffMs;
            }
            if ($shouldDelete) {
                $conn->del($this->hashPrefix . $uuid);
                $conn->zrem($this->listKey, $uuid);
            }
        }

        // $hours=null (queue:flush) 时 listKey 也清空; $hours 非 null (prune) 时保留剩余
        if ($hours === null) {
            $conn->del($this->listKey);
        }
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
     *
     * v0.4.8: 返回 `mixed` 避免 phpstan 报 zadd/set 等 magic method 缺失
     * (Connection 父类通过 __call 转发到 phpredis/predis client, 静态分析看不到).
     *
     * @return \Illuminate\Redis\Connections\Connection
     */
    private function redis(): Connection
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
