/**
 * Time/Timestamp utilities for test data
 *
 * Provides normalized timestamp calculations for consistent time-based testing.
 * All timestamps are normalized to milliseconds (floored to nearest second).
 */

/**
 * Normalize a timestamp to milliseconds (floor to nearest second)
 * Ensures consistent timestamp format across all test data
 */
export function getNormalizedTimestamp(now: number = Date.now()): number {
  return Math.floor(now / 1000) * 1000;
}

/**
 * Calculate a timestamp from a given number of days ago, normalized
 * Useful for testing data age-based logic
 */
export function getTimestampDaysAgo(days: number): number {
  const now = Date.now();
  const daysAgo = now - days * 24 * 60 * 60 * 1000;
  return getNormalizedTimestamp(daysAgo);
}

/**
 * Get start of day (00:00:00 UTC) from a given number of days ago
 * Useful for comparing operation timestamps
 */
export function getStartOfDay(daysAgo: number = 0): number {
  const date = new Date();
  date.setUTCHours(0, 0, 0, 0);
  date.setDate(date.getDate() - daysAgo);
  return date.getTime();
}

/**
 * Format a date as YYYY-MM-DD string for test IDs
 * Useful for building testid selectors based on dates
 */
export function getDateString(daysAgo: number = 0): string {
  const date = new Date();
  date.setDate(date.getDate() - daysAgo);
  return date.toISOString().split('T')[0];
}
