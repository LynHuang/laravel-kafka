<?php

declare(strict_types=1);

namespace LaravelKafka\Producer;

use LaravelKafka\Config\KafkaConfig;
use RdKafka\Conf;
use RdKafka\Producer as RdKafkaProducer;

/**
 * Producer 工厂（v0.1 单例工厂）。
 *
 * ## 角色
 *
 * 根据 {@see KafkaConfig} 构造 {@see Producer} 实例。
 * 单 connection 内单例缓存（`$config->name()` 为 key），**不**重复 `new RdKafka\Producer`。
 *
 * ## 业务方使用
 *
 * ```php
 * $factory = app(\LaravelKafka\Producer\ProducerFactory::class);
 * $producer = $factory->make($config);  // 同 config 第二次调返回同一实例
 * ```
 *
 * ## 资源管理
 *
 * - `flushAll()` 在 worker 退出前调用（kafka:work 信号处理）
 * - 单 connection 多个 consumer 共享同一 producer（节省 librdkafka 资源）
 *
 * ## 与 mateusjunges 的差异
 *
 * mateusjunges 没有独立 Factory 类，把 `Conf` 构造逻辑塞进 Manager；
 * 我们用独立 Factory，单测可以单独 mock。
 */
final class ProducerFactory
{
    /**
     * 单例缓存（key = config name）。
     *
     * @var array<string, Producer>
     */
    private array $instances = [];

    /**
     * 获取（或新建）Producer 实例。
     *
     * @param KafkaConfig $config 连接配置
     * @return Producer 单例（同一 config 多次调用返回同一实例）
     */
    public function make(KafkaConfig $config): Producer
    {
        $key = $config->name();
        if (! isset($this->instances[$key])) {
            $this->instances[$key] = $this->build($config);
        }
        return $this->instances[$key];
    }

    /**
     * flush 所有缓存的 Producer（worker 退出时调用）。
     *
     * 静默吞掉 `flush` 失败（仅 error_log），因为：
     *  - flush 失败只影响 in-flight 消息是否落地
     *  - 主流程已经走到最后，抛异常也救不回来
     *
     * @param int $timeoutMs 每个 producer 单独的超时（默认 10s）
     * @return void
     */
    public function flushAll(int $timeoutMs = 10000): void
    {
        foreach ($this->instances as $producer) {
            try {
                $producer->flush($timeoutMs);
            } catch (\Throwable $e) {
                // 静默：flush 失败只影响排空，不影响主流程
                error_log('[laravel-kafka] flush failed: ' . $e->getMessage());
            }
        }
    }

    /**
     * 构造新 Producer（首次 make 时调）。
     *
     * 步骤：
     *  1. 从 `KafkaConfig::toProducerRdKafkaConfig()` 拿 librdkafka 原始配置
     *  2. 设 error / log 回调（默认 error_log，业务方可重写）
     *  3. 调 `Producer::fromConf()` 绑定 delivery report 回调
     *
     * @param KafkaConfig $config
     * @return Producer
     */
    private function build(KafkaConfig $config): Producer
    {
        $conf = new Conf();
        $rdConfig = $config->toProducerRdKafkaConfig();
        foreach ($rdConfig as $k => $v) {
            $conf->set((string) $k, (string) $v);
        }
        // 错误回调
        $conf->setErrorCb(function ($kafka, $err, $reason) {
            // 仅记录到 PHP 错误日志；业务层订阅通过事件
            error_log(sprintf(
                '[laravel-kafka] producer error: code=%d reason=%s',
                $err,
                (string) $reason
            ));
        });
        // 日志回调（可选，默认丢弃）
        $conf->setLogCb(function ($kafka, $level, $facility, $message) {
            if ((int) $level <= 3) {
                error_log(sprintf(
                    '[laravel-kafka] librdkafka log: level=%d facility=%s message=%s',
                    $level,
                    (string) $facility,
                    (string) $message
                ));
            }
        });

        return Producer::fromConf($conf);
    }
}
