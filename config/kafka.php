<?php

declare(strict_types=1);

return [

    /*
    |--------------------------------------------------------------------------
    | 默认连接
    |--------------------------------------------------------------------------
    |
    | 在 `config/queue.php` 里通过 `QUEUE_CONNECTION=kafka` 切到本驱动时，
    | 本扩展按 `kafka.connections.<default>` 读取 Kafka 集群配置。
    |
    */

    'default' => env('KAFKA_CONNECTION', 'default'),

    /*
    |--------------------------------------------------------------------------
    | 连接配置
    |--------------------------------------------------------------------------
    |
    | 支持多 connection（与 Laravel queue connection 同构）。
    | 每个连接都包含完整独立的 broker / topic / producer / consumer / failed / delay / replay 配置。
    |
    */

    'connections' => [

        'default' => [

            // 基础
            'brokers'   => env('KAFKA_BROKERS', 'localhost:9092'),
            'client_id' => env('KAFKA_CLIENT_ID', 'laravel-kafka'),
            'protocol'  => env('KAFKA_PROTOCOL', 'PLAINTEXT'),    // PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL

            'sasl' => [
                'mechanism' => env('KAFKA_SASL_MECHANISM'),        // PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
                'username'  => env('KAFKA_SASL_USERNAME'),
                'password'  => env('KAFKA_SASL_PASSWORD'),
            ],

            'ssl' => [
                'ca_location'   => env('KAFKA_SSL_CA'),
                'cert_location' => env('KAFKA_SSL_CERT'),
                'key_location'  => env('KAFKA_SSL_KEY'),
                'key_password'  => env('KAFKA_SSL_KEY_PASSWORD'),
            ],

            // 默认 topic（Laravel 没显式指定 queue 时落到这里）
            'queue'  => env('KAFKA_DEFAULT_TOPIC', 'laravel-jobs'),

            // 队列名 → topic 映射
            // 留空表示用队列名当 topic 名
            'topics' => [
                // 'emails'  => 'app.emails',
                // 'reports' => 'app.reports',
            ],

            /*
            |----------------------------------------------------------------------
            | 生产者配置
            |----------------------------------------------------------------------
            |
            | 字段透传给 librdkafka，参考：
            |   https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
            |
            */

            'producer' => [
                'compression'        => env('KAFKA_PRODUCER_COMPRESSION', 'snappy'),   // none | gzip | snappy | lz4 | zstd
                'batch_size'         => (int) env('KAFKA_PRODUCER_BATCH_SIZE', 10000),  // bytes
                'linger_ms'          => (int) env('KAFKA_PRODUCER_LINGER_MS', 5),
                'request_timeout_ms' => (int) env('KAFKA_PRODUCER_REQUEST_TIMEOUT_MS', 30000),
                'message_timeout_ms' => (int) env('KAFKA_PRODUCER_MESSAGE_TIMEOUT_MS', 30000),
                'enable_idempotence' => (bool) env('KAFKA_PRODUCER_IDEMPOTENCE', true),
                'acks'               => env('KAFKA_PRODUCER_ACKS', 'all'),
            ],

            /*
            |----------------------------------------------------------------------
            | 消费者 / Worker 配置
            |----------------------------------------------------------------------
            */

            'consumer' => [
                'group_id'              => env('KAFKA_GROUP_ID', 'laravel-default'),
                'auto_offset_reset'     => env('KAFKA_AUTO_OFFSET_RESET', 'error'),     // earliest | latest | error
                'enable_auto_commit'    => false,                                        // 必须 false，由 Job::delete() 手动 commit
                'max_poll_interval_ms'  => (int) env('KAFKA_MAX_POLL_INTERVAL_MS', 300000),
                'session_timeout_ms'    => (int) env('KAFKA_SESSION_TIMEOUT_MS', 45000),
                'heartbeat_interval_ms' => (int) env('KAFKA_HEARTBEAT_INTERVAL_MS', 3000),
                'fetch_min_bytes'       => (int) env('KAFKA_FETCH_MIN_BYTES', 1),
                'fetch_max_bytes'       => (int) env('KAFKA_FETCH_MAX_BYTES', 52428800), // 50MB
                'isolation_level'       => env('KAFKA_ISOLATION_LEVEL', 'read_committed'),
            ],

            /*
            |----------------------------------------------------------------------
            | 失败处理（v0.1 三模式可配）
            |----------------------------------------------------------------------
            |
            | database: 失败写 failed_jobs 表（兼容 Laravel queue:failed 命令）
            | dlq:      失败写 DLQ topic
            | hybrid:   重试用 database 写明细，超限 / 致命异常双写 database + DLQ
            |
            */

            'failed' => [
                'driver'   => env('KAFKA_FAILED_DRIVER', 'hybrid'),

                // database / hybrid 模式专用
                'database' => [
                    'table'      => env('KAFKA_FAILED_TABLE', 'failed_jobs'),
                    'connection' => env('KAFKA_FAILED_DB_CONNECTION'),
                ],

                // dlq / hybrid 模式专用
                'dlq' => [
                    'topic'             => env('KAFKA_DLQ_TOPIC'),                  // null = 自动拼接 <default_topic>.dlq
                    'auto_topic_suffix' => env('KAFKA_DLQ_AUTO_SUFFIX', '.dlq'),
                    'retention_ms'      => (int) env('KAFKA_DLQ_RETENTION_MS', 1209600000), // 14d
                ],

                // hybrid 模式策略
                'hybrid' => [
                    'fatal_exceptions' => [
                        // \LaravelKafka\Exceptions\SerializationException::class,
                        // \Illuminate\Validation\ValidationException::class,
                    ],
                    'max_attempts'           => (int) env('KAFKA_MAX_ATTEMPTS', 3),
                    'trace_truncate_bytes'   => (int) env('KAFKA_TRACE_TRUNCATE', 32768),
                    'message_truncate_bytes' => (int) env('KAFKA_MESSAGE_TRUNCATE', 4096),
                ],
            ],

            /*
            |----------------------------------------------------------------------
            | 延迟消息（v0.2 启用）
            |----------------------------------------------------------------------
            */

            'delay' => [
                'strategy' => env('KAFKA_DELAY_STRATEGY', 'time_wheel'),  // time_wheel | requeue
                'tiers'    => [5, 30, 60, 300, 1800, 3600, 86400],         // seconds
            ],

            /*
            |----------------------------------------------------------------------
            | 回溯（v0.2 启用）
            |----------------------------------------------------------------------
            */

            'replay' => [
                'preserve_partition' => true,
            ],
        ],

    ],

];
