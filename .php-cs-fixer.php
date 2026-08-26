<?php

declare(strict_types=1);

/*
 * PHP-CS-Fixer 配置。
 * 遵循 PER Coding Style 2.0（PSR-12 升级版），适配 Laravel 生态。
 */

$finder = PhpCsFixer\Finder::create()
    ->in(__DIR__ . '/src')
    ->in(__DIR__ . '/tests')
    ->name('*.php')
    ->ignoreDotFiles(true)
    ->ignoreVCS(true);

return (new PhpCsFixer\Config())
    ->setRiskyAllowed(true)
    ->setRules([
        '@PER' => true,
        '@PER:risky' => true,
        '@PHP71Migration' => true,
        '@PHP74Migration' => true,
        'ordered_imports' => [
            'sort_algorithm' => 'alpha',
            'imports_order' => ['class', 'function', 'const'],
        ],
        'php_unit_method_casing' => ['case' => 'camel_case'],
        'php_unit_test_case_static_method_calls' => ['call_type' => 'self'],
        'no_unused_imports' => true,
        'single_quote' => true,
        // v0.4.1 round 7: PHP 7.4 不支持函数调用/方法调用参数列表的 trailing comma
        // (PHP 8.0+ 特性). 仅保留 arrays (PHP 7.4 已支持).
        'trailing_comma_in_multiline' => [
            'elements' => ['arrays'],
        ],
        'declare_strict_types' => true,
        'array_syntax' => ['syntax' => 'short'],
        'no_useless_else' => true,
        'no_useless_return' => true,
        'ordered_class_elements' => [
            'order' => [
                'constant_public',
                'constant_protected',
                'constant_private',
                'property_public_static',
                'property_protected_static',
                'property_private_static',
                'property_public',
                'property_protected',
                'property_private',
                'method_public_static',
                'method_protected_static',
                'method_private_static',
                'construct',
                'destruct',
                'magic',
                'phpunit',
                'method_public',
                'method_protected',
                'method_private',
            ],
        ],
    ])
    ->setFinder($finder)
    ->setCacheFile(__DIR__ . '/.php-cs-fixer.cache');
