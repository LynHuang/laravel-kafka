<?php

declare(strict_types=1);

namespace LaravelKafka\Support;

/**
 * W3C Trace Context 构造器（v0.2 升级版）。
 *
 * ## 格式（参考 W3C Trace Context Level 1）
 *
 * ```
 * traceparent: 00-<32hex trace-id>-<16hex parent-id>-<2hex flags>
 * example:    00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
 * ```
 *
 * 字段：
 *  - `version`：固定 `00`
 *  - `trace-id`：32 hex 字符（128 bit），标识整个调用链
 *  - `parent-id`：16 hex 字符（64 bit），标识当前 span
 *  - `flags`：2 hex 字符，`01` = sampled（默认），`00` = not sampled
 *
 * ## 升级动机
 *
 * v0.1 `TraceContext::next()` 只生成 16 字符 hex（不够 W3C 32 字符要求），
 * v0.2 升级到完整 W3C 标准，跨服务追踪更通用。
 *
 * 同时保留 v0.1 `x-trace-id` 16 字符 header（用 traceparent 的前 16 hex），
 * 保持向后兼容。
 *
 * ## flags = `01` 硬编码
 *
 * 当前 v0.2 全采样（任何 trace 都进系统）。v0.4 评估可配置（按概率采样 / 强制采样 / 关闭）。
 *
 * @see https://www.w3.org/TR/trace-context/ W3C 规范
 */
final class TraceContext
{
    /**
     * W3C Trace Context 版本号（当前固定 `00`）。
     *
     * 未来 W3C 升级到 01 时，本类会做 version-specific 解析。
     */
    private const VERSION = '00';

    /**
     * flags 位：`01` = sampled（trace 进系统），`00` = not sampled。
     */
    private const FLAGS_SAMPLED = '01';

    /**
     * 生成全新的 W3C traceparent 字符串（每次调用都不同）。
     *
     * trace-id 32 hex = 2^128 空间，碰撞概率忽略不计。
     * parent-id 16 hex = 2^64 空间，业务方每秒百万次也几乎不撞。
     *
     * @return string 形如 `00-<32hex>-<16hex>-01`
     */
    public static function next(): string
    {
        $traceId = self::randomHex(32);
        $parentId = self::randomHex(16);
        return self::buildTraceparent($traceId, $parentId);
    }

    /**
     * 派生子 span（同一 trace 内不同处理阶段的标识）。
     *
     * 用法：业务方在跨服务调用时，把上游传下来的 `traceparent` 透传进来，
     *       调用 `child()` 派生子 span 后注入下游消息。
     *
     * @param string $parentTraceparent 上游的 traceparent 字符串
     * @return string 新的 traceparent（保留 trace-id，新 parent-id）
     *         如果入参非法则回退到 `next()`
     */
    public static function child(string $parentTraceparent): string
    {
        $parsed = self::parse($parentTraceparent);
        if ($parsed === null) {
            return self::next();
        }
        // 保留 trace-id（同一调用链），生成新 parent-id（区分处理阶段）
        return self::buildTraceparent($parsed['trace_id'], self::randomHex(16));
    }

    /**
     * 解析 traceparent 字符串。
     *
     * 严格校验：4 段 + 版本 `00` + trace-id 32 hex + parent-id 16 hex + flags 2 hex。
     * 任一不满足返回 `null`（不抛异常，方便 `child()` 兜底）。
     *
     * @param string $traceparent 待解析的字符串
     * @return array{trace_id: string, parent_id: string, flags: string}|null 解析失败返回 null
     */
    public static function parse(string $traceparent): ?array
    {
        $parts = explode('-', $traceparent);
        if (count($parts) !== 4) {
            return null;
        }
        [$version, $traceId, $parentId, $flags] = $parts;
        if ($version !== self::VERSION) {
            return null;
        }
        if (! preg_match('/^[0-9a-f]{32}$/', $traceId)) {
            return null;
        }
        if (! preg_match('/^[0-9a-f]{16}$/', $parentId)) {
            return null;
        }
        if (! preg_match('/^[0-9a-f]{2}$/', $flags)) {
            return null;
        }
        return [
            'trace_id' => $traceId,
            'parent_id' => $parentId,
            'flags' => $flags,
        ];
    }

    /**
     * 提取 trace-id 的前 16 hex（与 v0.1 `x-trace-id` 兼容）。
     *
     * v0.2 同时写 `traceparent` (32 hex) + `x-trace-id` (16 hex)，
     * 业务方读 `x-trace-id` 仍能拿到 v0.1 兼容的 16 字符 ID。
     *
     * @param string $traceparent traceparent 字符串
     * @return string|null 16 字符 hex，解析失败返回 null
     */
    public static function shortTraceId(string $traceparent): ?string
    {
        $parsed = self::parse($traceparent);
        return $parsed === null ? null : substr($parsed['trace_id'], 0, 16);
    }

    /**
     * 内部：生成指定长度的随机 hex 字符串。
     *
     * 用 `random_bytes`（密码学安全 PRNG），不是 `rand`（弱 PRNG）。
     *
     * @param int $length hex 字符数（必须是正偶数）
     * @return string hex 字符串
     */
    private static function randomHex(int $length): string
    {
        $bytes = (int) ceil($length / 2);
        return substr(bin2hex(random_bytes($bytes)), 0, $length);
    }

    /**
     * 内部：拼装 W3C 格式 traceparent 字符串。
     *
     * @param string $traceId  32 hex
     * @param string $parentId 16 hex
     * @return string 形如 `00-<traceId>-<parentId>-01`
     */
    private static function buildTraceparent(string $traceId, string $parentId): string
    {
        return self::VERSION . '-' . $traceId . '-' . $parentId . '-' . self::FLAGS_SAMPLED;
    }
}
