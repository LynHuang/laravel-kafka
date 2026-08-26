<?php

declare(strict_types=1);

namespace LaravelKafka\Replay;

use LaravelKafka\Exceptions\KafkaException;

/**
 * 时间窗口解析器（v0.3 Step 4）。
 *
 * ## 用途
 *
 * `kafka:replay --from="-1h" --to=now` 等 CLI 参数需要解析成 Unix timestamp。
 *
 * ## 支持格式
 *
 * | 输入 | 含义 |
 * | --- | --- |
 *  | `now` | 当前时间（`time()`） |
 *  | `-1h` | 1 小时前 |
 *  | `-30m` | 30 分钟前 |
 *  | `-7d` | 7 天前 |
 *  | `-60s` | 60 秒前 |
 *  | `1700000000` | 绝对 Unix timestamp |
 *  | `2026-08-25 10:00:00` | 绝对时间字符串（`strtotime` 解析） |
 *
 * ## 单位
 *
 * - `s` = 秒
 * - `m` = 分钟（60s）
 * - `h` = 小时（3600s）
 * - `d` = 天（86400s）
 */
final class TimeWindowParser
{
    /**
     * 把字符串解析成 Unix timestamp。
     *
     * @param string $value 时间字符串
     * @param int|null $now 当前时间（注入用于测试，null = `time()`）
     * @return int Unix timestamp
     * @throws KafkaException 无法解析时
     */
    public function parse(string $value, ?int $now = null): int
    {
        $value = trim($value);
        if ($value === '') {
            throw new KafkaException('TimeWindowParser value must not be empty.');
        }

        $now = $now ?? time();

        // "now" → 当前时间
        if ($value === 'now') {
            return $now;
        }

        // 相对时间 "-1h" / "-30m" / "-7d" / "-60s"
        if (preg_match('/^(-?\d+)([smhd])$/', $value, $m)) {
            $offset = (int) $m[1];
            $unit = $m[2];
            $multipliers = ['s' => 1, 'm' => 60, 'h' => 3600, 'd' => 86400];
            return $now + $offset * $multipliers[$unit];
        }

        // 绝对 Unix timestamp（纯数字）
        if (ctype_digit($value)) {
            return (int) $value;
        }

        // 绝对时间字符串
        $ts = strtotime($value);
        if ($ts !== false) {
            return $ts;
        }

        throw new KafkaException(sprintf(
            'TimeWindowParser cannot parse: "%s" (supported: "now", "-1h", "1700000000", "2026-08-25 10:00:00")',
            $value
        ));
    }
}
