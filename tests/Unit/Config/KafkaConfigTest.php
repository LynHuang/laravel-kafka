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

        $this->assertSame('default', $cfg->name());
        $this->assertSame('localhost:9092', $cfg->brokers());
        $this->assertSame('jobs', $cfg->defaultTopic());
        $this->assertSame('laravel-kafka', $cfg->clientId());
        $this->assertSame('PLAINTEXT', $cfg->protocol());
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
        $this->assertSame('app.emails', $cfg->resolveTopic('emails'));
    }

    public function testResolveTopicFallsBackToName(): void
    {
        $cfg = KafkaConfig::fromArray('default', [
            'brokers' => 'x:9092',
            'queue' => 'jobs',
        ]);
        $this->assertSame('reports', $cfg->resolveTopic('reports'));
    }

    public function testResolveTopicFallsBackToDefault(): void
    {
        $cfg = KafkaConfig::fromArray('default', [
            'brokers' => 'x:9092',
            'queue' => 'jobs',
        ]);
        $this->assertSame('jobs', $cfg->resolveTopic(null));
        $this->assertSame('jobs', $cfg->resolveTopic(''));
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
        $this->assertSame('localhost:9092', $conf['bootstrap.servers']);
        $this->assertSame('app-1', $conf['client.id']);
        $this->assertSame('gzip', $conf['compression']);
        $this->assertSame('10', $conf['linger_ms']);
        $this->assertSame('true', $conf['enable_idempotence']);
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
        $this->assertSame('g-1', $conf['group.id']);
        $this->assertSame('false', $conf['enable.auto.commit']);
        $this->assertSame('earliest', $conf['auto.offset.reset']);
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
        $this->assertSame('SASL_PLAINTEXT', $conf['security.protocol']);
        $this->assertSame('PLAIN', $conf['sasl.mechanism']);
        $this->assertSame('u', $conf['sasl.username']);
        $this->assertSame('p', $conf['sasl.password']);
    }
}
