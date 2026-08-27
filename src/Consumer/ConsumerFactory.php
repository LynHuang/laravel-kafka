<?php

declare(strict_types=1);

namespace LaravelKafka\Consumer;

use LaravelKafka\Config\KafkaConfig;
use RdKafka\Conf;
use RdKafka\KafkaConsumer as RdKafkaConsumer;

/**
 * Consumer 工厂（v0.1 单例工厂，与 {@see \LaravelKafka\Producer\ProducerFactory} 对称）。
 *
 * ## 角色
 *
 * 根据 {@see KafkaConfig} 构造 {@see Consumer} 实例。
 * 单 connection 内单例缓存（`$config->name()` 为 key）。
 *
 * ## Rebalance 回调
 *
 * 内置 `setRebalanceCb`：
 *  - `RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS` → `assign($partitions)` 接管
 *  - `RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS` → `assign(null)` 主动放弃
 *  - 其他错误码 → `assign(null)` + 错误日志
 *
 * v0.2 增强：rebalance 时**主动 commit offset**（避免重平衡后消息被重投）。
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 把 rebalance 逻辑直接写在 `Consumer` 类里；我们拆出独立 Factory + 回调。
 */
final class ConsumerFactory
{
    /**
     * 单例缓存（key = config name）。
     *
     * @var array<string, Consumer>
     */
    private array $instances = [];

    /**
     * 获取（或新建）Consumer 实例。
     *
     * 第二次同 config 调用时**不**支持改 subscription（返回第一次缓存的实例）。
     * 想换 subscription → `closeAll()` 后再 `make()`。
     *
     * @param KafkaConfig $config
     * @param Subscription|null $subscription null = 用 `defaultTopic` 单 topic
     * @return Consumer
     */
    public function make(KafkaConfig $config, ?Subscription $subscription = null): Consumer
    {
        $key = $config->name();
        if (! isset($this->instances[$key])) {
            $subscription ??= new Subscription([$config->defaultTopic()]);
            $this->instances[$key] = $this->build($config, $subscription);
        }
        return $this->instances[$key];
    }

    /**
     * 关闭并清空所有缓存的 Consumer。
     *
     * 主要用于：
     *  - 单元测试 tearDown
     *  - 运行时切换 connection（业务方 reload 配置后想重建 consumer）
     *
     * @return void
     */
    public function closeAll(): void
    {
        foreach ($this->instances as $consumer) {
            $consumer->close();
        }
        $this->instances = [];
    }

    /**
     * 构造新 Consumer（首次 make 时调）。
     *
     * 步骤：
     *  1. 从 `KafkaConfig::toConsumerRdKafkaConfig()` 拿 librdkafka 原始配置
     *  2. 设 error 回调
     *  3. 设 rebalance 回调（处理 assign / revoke）
     *  4. `new RdKafkaConsumer($conf)` + 包装成 {@see Consumer}
     *
     * @param KafkaConfig $config
     * @param Subscription $subscription
     * @return Consumer
     */
    private function build(KafkaConfig $config, Subscription $subscription): Consumer
    {
        $conf = new Conf();
        $rdConfig = $config->toConsumerRdKafkaConfig();
        foreach ($rdConfig as $k => $v) {
            $conf->set((string) $k, (string) $v);
        }
        // 错误回调
        $conf->setErrorCb(function ($kafka, $err, $reason) {
            error_log(sprintf(
                '[laravel-kafka] consumer error: code=%d reason=%s',
                $err,
                (string) $reason
            ));
        });
        // rebalance 回调：记录日志；v0.2 完善
        $conf->setRebalanceCb(function (RdKafkaConsumer $kafka, $err, ?array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    $kafka->assign($partitions);
                    error_log(sprintf(
                        '[laravel-kafka] rebalance: assigned %d partition(s)',
                        is_array($partitions) ? count($partitions) : 0
                    ));
                    break;
                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    $kafka->assign(null);
                    error_log('[laravel-kafka] rebalance: revoked partitions');
                    break;
                default:
                    $kafka->assign(null);
                    error_log(sprintf('[laravel-kafka] rebalance: error code=%d', $err));
            }
        });

        $kafka = new RdKafkaConsumer($conf);
        return new Consumer($kafka, $subscription);
    }
}
