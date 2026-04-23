/**
 * Random string utilities for generating unique test data.
 *
 * Use `withTimestamp` when you need the creation time embedded in the name
 * (helpful for debugging stale data). Use `withRandomSuffix` when you need
 * a shorter unique suffix.
 */

/** Generate a random alphanumeric string of the given length. */
export function generateRandomString(length: number = 8): string {
  return Math.random().toString(36).substring(2, length + 2);
}

/**
 * Append a timestamp suffix in YYYYMMDD_HHmmss format.
 *
 * Example: withTimestamp('test_dataset') → 'test_dataset_20260408_143022'
 */
export function withTimestamp(base: string): string {
  const now = new Date();
  const pad = (n: number) => String(n).padStart(2, '0');
  const ts = [
    now.getFullYear(),
    pad(now.getMonth() + 1),
    pad(now.getDate()),
    '_',
    pad(now.getHours()),
    pad(now.getMinutes()),
    pad(now.getSeconds()),
  ].join('');
  return `${base}_${ts}`;
}

/**
 * Append a short random suffix.
 *
 * Example: withRandomSuffix('test_user') → 'test_user_a3f7k2b1'
 */
export function withRandomSuffix(base: string): string {
  return `${base}_${generateRandomString()}`;
}
