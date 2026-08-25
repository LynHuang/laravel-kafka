<?php

declare(strict_types=1);

namespace LaravelKafka\Support;

/**
 * 字符串工具（v0.1 最小集）。
 *
 * ## 角色
 *
 * 只放本包需要、但 Laravel `Illuminate\Support\Str` 不直接提供的工具：
 *  - 字节安全的 `truncate()`（不是字符安全，**严格按字节**截断）
 *  - 简单 `mask()`（中间替换成 `*`，保留头尾）
 *  - `isUuid()`（v0.4 Avro 评估用）
 *
 * ## 为什么不直接用 Laravel Str
 *
 * Laravel `Str::limit()` 是**字符**安全（mb_* 函数），Kafka 消息体是**字节**串，
 * 用字符安全截断会破坏多字节 payload。所以本类提供字节版。
 *
 * ## v0.1 限制
 *
 * 只 3 个方法。如果业务方需要更复杂的字符串处理（slug / camel / snake），
 * 走 `Illuminate\Support\Str`，**不**在本类重复造。
 */
final class Str
{
    /**
     * 字节安全截断（不是字符安全）。
     *
     * 业务方场景：把超长 payload 写日志前截断（避免日志爆炸），
     * **不**用字符安全是防止多字节 UTF-8 被切一半产生乱码。
     *
     * @param string $value 待截断字符串
     * @param int $maxBytes 最大字节数（含 `$ellipsis`）
     * @param string $ellipsis 截断后缀（默认 `...`）
     * @return string 截断后的字符串（超长时末尾拼 `$ellipsis`）
     */
    public static function truncate(string $value, int $maxBytes, string $ellipsis = '...'): string
    {
        if (strlen($value) <= $maxBytes) {
            return $value;
        }
        $cutAt = max(0, $maxBytes - strlen($ellipsis));
        return substr($value, 0, $cutAt) . $ellipsis;
    }

    /**
     * 中间字符替换成 mask（保留头尾可见）。
     *
     * 业务方场景：日志里打印 connection string / token 时 mask 中间部分。
     *
     * 示例：`Str::mask('abcdefghij', 2, 2)` → `'ab******ij'`
     *
     * @param string $value 待 mask 字符串
     * @param int $visibleStart 头部可见字符数（默认 2）
     * @param int $visibleEnd 尾部可见字符数（默认 2）
     * @param string $mask 替换字符（默认 `*`）
     * @return string mask 后的字符串
     */
    public static function mask(string $value, int $visibleStart = 2, int $visibleEnd = 2, string $mask = '*'): string
    {
        $len = strlen($value);
        if ($len <= $visibleStart + $visibleEnd) {
            return str_repeat($mask, $len);
        }
        $head = substr($value, 0, $visibleStart);
        $tail = substr($value, -$visibleEnd);
        $middle = str_repeat($mask, $len - $visibleStart - $visibleEnd);
        return $head . $middle . $tail;
    }

    /**
     * 是否为标准 UUID（8-4-4-4-12 hex 字符）。
     *
     * v0.4 评估 Avro 时，用于校验 schema id 格式。
     *
     * @param string $value 待校验字符串
     * @return bool true = 是合法 UUID
     */
    public static function isUuid(string $value): bool
    {
        return (bool) preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i', $value);
    }
}
