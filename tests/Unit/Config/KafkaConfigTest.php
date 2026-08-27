<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Config;

use LaravelKafka\Config\KafkaConfig;
use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Tests\TestCase;

/**
 * @covers \LaravelKafka\Config\KafkaConfig
 */
final class KafkaConfigTest extends TestCase
{
    public function testFromArrayWithDefaults(): void
    {
        $cfg = KafkaConfig::fromArray('default', [
            'brokers' => 'localhost:9092',
            'queue' => 'jobs',
        ]);

        self::assertSame('default', $cfg->name());
        self::assertSame('localhost:9092', $cfg->brokers());
        self::assertSame('jobs', $cfg->defaultTopic());
        self::assertSame('laravel-kafka', $cfg->clientId());
        self::assertSame('PLAINTEXT', $cfg->protocol());
    }

    public function testEmptyBrokersThrows(): void
    {
        $this->expectException(KafkaException::class);
        $this->expectExceptionMessage('brokers must not be empty');
        KafkaConfig::fromArray('default', ['queue' => 'jobs']);
    }

    public function testEmptyTopicThrows(): void
    {
        $this->expectException(KafkaException::class);
        $this->expectExceptionMessage('default topic must not be empty');
        KafkaConfig::fromArray('default', ['brokers' => 'x:9092']);
    }

    public function testInvalidProtocolThrows(): void
    {
        $this->expectException(KafkaException::class);
        $this->expectExceptionMessage('Invalid Kafka protocol');
        KafkaConfig::fromArray('default', [
            'brokers' => 'x:9092',
            'queue' => 'jobs',
            'protocol' => 'WIRED',
        ]);
    }

    public function testResolveTopicPrefersMap(): void
    {
        $cfg = KafkaConfig::fromArray('default', [
            'brokers' => 'x:9092',
            'queue' => 'jobs',
            'topics' => ['emails' => 'app.emails'],
        ]);
        self::assertSame('app.emails', $cfg->resolveTopic('emails'));
    }

    public function testResolveTopicFallsBackToName(): void
    {
        $cfg = KafkaConfig::fromArray('default', [
            'brokers' => 'x:9092',
            'queue' => 'jobs',
        ]);
        self::assertSame('reports', $cfg->resolveTopic('reports'));
    }

    public function testResolveTopicFallsBackToDefault(): void
    {
        $cfg = KafkaConfig::fromArray('default', [
            'brokers' => 'x:9092',
            'queue' => 'jobs',
        ]);
        self::assertSame('jobs', $cfg->resolveTopic(null));
        self::assertSame('jobs', $cfg->resolveTopic(''));
    }

    public function testToProducerRdKafkaConfig(): void
    {
        $cfg = KafkaConfig::fromArray('default', [
            'brokers' => 'localhost:9092',
            'client_id' => 'app-1',
            'queue' => 'jobs',
            'producer' => [
                'compression' => 'gzip',
                'linger_ms' => 10,
                'enable_idempotence' => true,
            ],
        ]);
        $conf = $cfg->toProducerRdKafkaConfig();
        self::assertSame('localhost:9092', $conf['bootstrap.servers']);
        self::assertSame('app-1', $conf['client.id']);
        // v0.4.3 hotfix: 业务方友好名 -> librdkafka 原生名 (compression.type / linger.ms / enable.idempotence)
        self::assertSame('gzip', $conf['compression.type']);
        self::assertSame('10', $conf['linger.ms']);
        self::assertSame('true', $conf['enable.idempotence']);
    }

    public function testToConsumerRdKafkaConfig(): void
    {
        $cfg = KafkaConfig::fromArray('default', [
            'brokers' => 'localhost:9092',
            'queue' => 'jobs',
            'consumer' => [
                'group_id' => 'g-1',
                'auto_offset_reset' => 'earliest',
            ],
        ]);
        $conf = $cfg->toConsumerRdKafkaConfig();
        self::assertSame('g-1', $conf['group.id']);
        self::assertSame('false', $conf['enable.auto.commit']);
        self::assertSame('earliest', $conf['auto.offset.reset']);
    }

    public function testSaslConfigInjected(): void
    {
        $cfg = KafkaConfig::fromArray('default', [
            'brokers' => 'localhost:9092',
            'queue' => 'jobs',
            'protocol' => 'SASL_PLAINTEXT',
            'sasl' => [
                'mechanism' => 'PLAIN',
                'username' => 'u',
                'password' => 'p',
            ],
        ]);
        $conf = $cfg->toProducerRdKafkaConfig();
        self::assertSame('SASL_PLAINTEXT', $conf['security.protocol']);
        self::assertSame('PLAIN', $conf['sasl.mechanism']);
        self::assertSame('u', $conf['sasl.username']);
        self::assertSame('p', $conf['sasl.password']);
    }
}
