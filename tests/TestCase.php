<?php

declare(strict_types=1);

namespace LaravelKafka\Tests;

use LaravelKafka\LaravelKafkaServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

/**
 * 单元测试基类。
 *
 * - 不依赖 Kafka 集群，纯逻辑测试
 * - 启动 Laravel Testbench 容器
 * - 注册 LaravelKafkaServiceProvider
 * - 提供常用 fixture（KafkaConfig / Message / Serializer）
 */
abstract class TestCase extends Orchestra
{
    /**
     * @param \Illuminate\Foundation\Application $app
     * @return array<int,string>
     */
    protected function getPackageProviders($app): array
    {
        return [
            LaravelKafkaServiceProvider::class,
        ];
    }

    /**
     * @param \Illuminate\Foundation\Application $app
     * @return array<string,class-string>
     */
    protected function getPackageAliases($app): array
    {
        return [
            'Kafka' => \LaravelKafka\Facades\Kafka::class,
        ];
    }

    /**
     * @param \Illuminate\Foundation\Application $app
     */
    protected function defineEnvironment($app): void
    {
        $app['config']->set('kafka.connections.default', [
            'brokers' => 'localhost:9092',
            'client_id' => 'laravel-kafka-test',
            'protocol' => 'PLAINTEXT',
            'queue' => 'laravel-jobs-test',
            'topics' => [],
            'producer' => [],
            'consumer' => ['group_id' => 'laravel-test'],
            'failed' => [
                'driver' => 'hybrid',
                'database' => ['table' => 'failed_jobs_test'],
                'dlq' => ['topic' => 'laravel-jobs-test.dlq'],
                'hybrid' => ['max_attempts' => 3],
            ],
            'delay' => [],
            'replay' => [],
        ]);
    }
}
