<?php

declare(strict_types=1);

namespace LaravelKafka\Tests\Unit\Queue\Failed;

use LaravelKafka\Exceptions\KafkaException;
use LaravelKafka\Queue\Failed\ExceptionClassRouter;
use LaravelKafka\Tests\TestCase;

/**
 * v0.3 Step 3: ExceptionClassRouter 单元测试。
 *
 * @covers \LaravelKafka\Queue\Failed\ExceptionClassRouter
 */
final class ExceptionClassRouterTest extends TestCase
{
    public function testRouteExactClassMatch(): void
    {
        $router = new ExceptionClassRouter(
            [TestFatalException::class => 'fatal-topic'],
            'default-topic'
        );

        $route = $router->route(new TestFatalException('boom'));
        $this->assertSame('fatal-topic', $route);
    }

    public function testRouteInheritanceMatch(): void
    {
        // 子类应该匹配父类
        $router = new ExceptionClassRouter(
            [TestParentException::class => 'parent-topic'],
            'default-topic'
        );

        $route = $router->route(new TestChildException('boom'));
        $this->assertSame('parent-topic', $route);
    }

    public function testRouteFallsBackToDefault(): void
    {
        $router = new ExceptionClassRouter(
            [TestFatalException::class => 'fatal-topic'],
            'default-topic'
        );

        // 抛一个未在 map 里的异常
        $route = $router->route(new \RuntimeException('unknown'));
        $this->assertSame('default-topic', $route);
    }

    public function testRouteEmptyMapAlwaysFallsBack(): void
    {
        $router = new ExceptionClassRouter([], 'default-topic');
        $route = $router->route(new \RuntimeException('any'));
        $this->assertSame('default-topic', $route);
    }

    public function testConstructorRejectsEmptyDefaultTopic(): void
    {
        $this->expectException(KafkaException::class);
        $this->expectExceptionMessage('defaultTopic must not be empty');
        new ExceptionClassRouter([], '');
    }

    public function testDefaultTopicGetter(): void
    {
        $router = new ExceptionClassRouter([], 'my-default');
        $this->assertSame('my-default', $router->defaultTopic());
    }

    public function testSizeReturnsMapSize(): void
    {
        $router = new ExceptionClassRouter(
            [
                \RuntimeException::class => 'a',
                \InvalidArgumentException::class => 'b',
            ],
            'default'
        );
        $this->assertSame(2, $router->size());
    }
}

/**
 * 测试 fixture：与测试同 namespace 避开 PSR-4 自动加载。
 */
class TestFatalException extends \RuntimeException
{
}
class TestParentException extends \RuntimeException
{
}
class TestChildException extends TestParentException
{
}
