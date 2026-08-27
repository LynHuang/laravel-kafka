<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Manager;

use LaravelKafka\Manager\KafkaManager;
use LaravelKafka\Tests\TestCase;

/**
 * @covers \LaravelKafka\Manager\KafkaManager
 */
final class KafkaManagerTest extends TestCase
{
    public function testInitialFakeModeFalse(): void
    {
        $manager = $this->app->make(KafkaManager::class);
        self::assertFalse($manager->isFake());
    }

    public function testFakeModeToggle(): void
    {
        $manager = $this->app->make(KafkaManager::class);
        self::assertFalse($manager->isFake());

        $manager->fake();
        self::assertTrue($manager->isFake());

        // fake 是一次性开关，不存在 "unfake"
        // 业务方应该用 $manager = $this->app->make(KafkaManager::class) 重新拿一个
    }

    public function testFakeModeSurvivesAcrossCalls(): void
    {
        $this->app->make(KafkaManager::class)->fake();
        // 第二次拿实例（singleton）状态保持
        $manager2 = $this->app->make(KafkaManager::class);
        self::assertTrue($manager2->isFake());
    }

    public function testFactoryInjectedViaContainer(): void
    {
        $manager = $this->app->make(KafkaManager::class);
        self::assertInstanceOf(KafkaManager::class, $manager);
    }
}
