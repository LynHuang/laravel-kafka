<?php

declare(strict_types=1);

namespace LaravelKafka\Horizon;

use LaravelKafka\Exceptions\KafkaException;

/**
 * Horizon 兼容的 metrics 记录器（v0.4 单任务）。
 *
 * ## 角色
 *
 * 让 `kafka:work` 处理的消息 metrics 写到 Redis 兼容 Horizon 的格式，
 * 业务方在 Horizon dashboard 自动看到 Kafka 队列的 throughput / runtime。
 *
 * ## Redis key 格式（与 Horizon 5.x 完全一致）
 *
 * | Key | 类型 | 内容 |
 * | --- | --- | --- |
 * | `<prefix>measured_queues` | Set | queue 名列表（带 `queue:` 前缀） |
 * | `<prefix>queue:<queueName>` | Hash | `{throughput: int, runtime: float}` |
 * | `<prefix>measured_jobs` | Set | job 类名列表（带 `job:` 前缀） |
 * | `<prefix>job:<className>` | Hash | `{throughput: int, runtime: float}` |
 * | `<prefix>snapshot:queue:<queueName>` | Sorted Set | 历史快照 |
 * | `<prefix>last_snapshot_at` | String | 最后 snapshot timestamp |
 *
 * `<prefix>` 默认 `horizon:`（业务方在 Horizon config 里改）。
 *
 * ## Lua 脚本
 *
 * 直接复用 Horizon 5.x 的 `LuaScripts::updateMetrics()`（见 [Horizon 源码][1]）：
 * ```lua
 * redis.call('hsetnx', KEYS[1], 'throughput', 0)
 * redis.call('sadd', KEYS[2], KEYS[1])
 * local hash = redis.call('hmget', KEYS[1], 'throughput', 'runtime')
 * local throughput = hash[1] + 1
 * local runtime = 0
 * if hash[2] then
 *     runtime = ((hash[1] * tonumber(hash[2])) + tonumber(ARGV[1])) / throughput
 * else
 *     runtime = tonumber(ARGV[1])
 * end
 * redis.call('hmset', KEYS[1], 'throughput', throughput, 'runtime', runtime)
 * ```
 *
 * [1]: https://github.com/laravel/horizon/blob/master/src/LuaScripts.php
 *
 * ## 依赖
 *
 * - `Illuminate\Contracts\Redis\Factory`（已在 `illuminate/contracts`）
 * - 实际 Redis 实现（`predis/predis` 或 `phpredis` + `illuminate/redis`）业务方自己装
 *   （装 Horizon 自然就齐了）
 *
 * ## 用法
 *
 * 业务方在 Laravel 项目里：
 *  1. 装 Horizon + 配 Redis `horizon` connection
 *  2. 跑 `kafka:work --horizon-metrics`（默认 prefix `horizon:`）
 *  3. 打开 `/horizon` dashboard 看到 Kafka 队列
 *
 * @see https://horizon.laravel.com/
 */
final class HorizonMetricsRecorder
{
    /**
     * Horizon 5.x `updateMetrics` Lua 脚本（逐字复制）。
     *
     * @var string
     */
    private const UPDATE_METRICS_LUA = <<<'LUA'
                redis.call('hsetnx', KEYS[1], 'throughput', 0)
                redis.call('sadd', KEYS[2], KEYS[1])
                local hash = redis.call('hmget', KEYS[1], 'throughput', 'runtime')
                local throughput = hash[1] + 1
                local runtime = 0
                if hash[2] then
                    runtime = ((hash[1] * tonumber(hash[2])) + tonumber(ARGV[1])) / throughput
                else
                    runtime = tonumber(ARGV[1])
                end
                redis.call('hmset', KEYS[1], 'throughput', throughput, 'runtime', runtime)
                return 1
        LUA;

    /**
     * 缓存 SCRIPT LOAD 的 SHA1 (跨调用复用, 避免每次重传 Lua).
     *
     * @var string|null
     */
    private static $scriptSha = null;
    /**
     * Redis 连接工厂（`Illuminate\Contracts\Redis\Factory`）。
     *
     * @var mixed
     */
    private $redis;

    /**
     * Horizon Redis 连接名（默认 `horizon`）。
     */
    private string $connection;

    /**
     * Horizon Redis key 前缀（默认 `horizon:`）。
     */
    private string $prefix;

    /**
     * @param mixed $redis Laravel Redis Factory（如 `Illuminate\Contracts\Redis\Factory`）
     * @param string $connection Horizon Redis 连接名（默认 `horizon`）
     * @param string $prefix Horizon Redis key 前缀（默认 `horizon:`）
     */
    public function __construct($redis, string $connection = 'horizon', string $prefix = 'horizon:')
    {
        if ($redis === null) {
            throw new KafkaException('HorizonMetricsRecorder: $redis (Redis Factory) cannot be null.');
        }
        $this->redis = $redis;
        $this->connection = $connection;
        $this->prefix = $prefix;
    }

    /**
     * 记录一个 queue 处理一次（increase throughput + 加权平均 runtime）。
     *
     * @param string $queue 物理 topic 名 / Laravel 逻辑 queue 名
     * @param float $runtimeMs 处理耗时（毫秒）
     * @return void
     */
    public function incrementQueue(string $queue, float $runtimeMs): void
    {
        $this->evalMetrics('queue:' . $queue, 'measured_queues', $runtimeMs);
    }

    /**
     * 记录一个 job 类处理一次。
     *
     * @param string $jobClass 完整类名（如 `App\Jobs\SendEmail`）
     * @param float $runtimeMs 处理耗时（毫秒）
     * @return void
     */
    public function incrementJob(string $jobClass, float $runtimeMs): void
    {
        $this->evalMetrics('job:' . $jobClass, 'measured_jobs', $runtimeMs);
    }

    /**
     * 拿 Redis key 前缀（含末尾 `:`）。
     */
    public function prefix(): string
    {
        return $this->prefix;
    }

    /**
     * 拿 Redis 连接名。
     */
    public function connection(): string
    {
        return $this->connection;
    }

    /**
     * 内部：执行 Horizon 兼容的 `updateMetrics` Lua 脚本。
     *
     * v0.4.4 hotfix: Laravel 8.x `PhpRedisConnection::eval()` 包装层用 eval 调用时
     * 实际**不执行** Lua 脚本 (返回 false 但不抛错, silent fail).
     * 修法: 穿透到 phpredis client (`$conn->client()`) 用 SCRIPT LOAD + EVALSHA 路径,
     *        跳过 Laravel 包装层 + phpredis eval 在 prefix 模式下的返回值 bug.
     *        Public 签名不变 (保持 unit test mock 不破).
     */
    private function evalMetrics(string $metricsKey, string $measuredSetKey, float $runtimeMs): void
    {
        $conn = $this->redis->connection($this->connection);
        // PHP 8.0+ 反对 ',' 作为小数点 → Lua 不接受 ','，替换为 '.'
        $runtimeArg = str_replace(',', '.', (string) $runtimeMs);

        $key1 = $this->prefix . $metricsKey;
        $key2 = $this->prefix . $measuredSetKey;

        if ($conn instanceof \Illuminate\Redis\Connections\PhpRedisConnection) {
            /** @var \Redis $client */
            $client = $conn->client();

            // SCRIPT LOAD 拿 SHA1 (跨调用缓存, 静态变量)
            if (self::$scriptSha === null) {
                $savedPrefix = $client->getOption(\Redis::OPT_PREFIX);
                $client->setOption(\Redis::OPT_PREFIX, '');
                self::$scriptSha = (string) $client->script('load', self::UPDATE_METRICS_LUA);
                $client->setOption(\Redis::OPT_PREFIX, $savedPrefix);
            }

            // EVALSHA — 直接执行已加载脚本, 不会触发 phpredis eval 返回值 bug
            $result = $client->evalsha(self::$scriptSha, [$key1, $key2, $runtimeArg], 2);
            if ($result === false) {
                // SCRIPT 可能被 Redis flush 掉, 重 load 后再试一次
                $savedPrefix = $client->getOption(\Redis::OPT_PREFIX);
                $client->setOption(\Redis::OPT_PREFIX, '');
                self::$scriptSha = (string) $client->script('load', self::UPDATE_METRICS_LUA);
                $client->setOption(\Redis::OPT_PREFIX, $savedPrefix);
                $result = $client->evalsha(self::$scriptSha, [$key1, $key2, $runtimeArg], 2);
                if ($result === false) {
                    throw new KafkaException(sprintf(
                        'HorizonMetricsRecorder::evalMetrics phpredis evalsha failed: %s (keys=%s, %s; arg=%s)',
                        (string) $client->getLastError(),
                        $key1,
                        $key2,
                        $runtimeArg
                    ));
                }
            }
            return;
        }

        // v0.4.8: PredisConnection 也走 fallback 路径 (predis 客户端继承自 Connection,
        // 没有显式 eval() 声明, 实际通过 __call 转发). 删冗余的 PredisConnection 分支.
        // Fallback: 未知 connection 类型, 试公共 eval
        /** @phpstan-ignore-next-line */
        $conn->eval(self::UPDATE_METRICS_LUA, 2, $key1, $key2, $runtimeArg);
    }
}
