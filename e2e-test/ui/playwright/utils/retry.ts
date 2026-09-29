import { DEFAULT_TIMEOUT, POLL_INTERVAL } from './constants';

export interface RetryOnFailOptions {
  onRetry?: () => Promise<void>;
  timeout?: number;
  interval?: number;
}

/**
 * Retry a block of `expect` assertions, calling `onRetry` between attempts.
 * Useful for waiting on ES-indexed changes — pass a page reload as `onRetry`
 * so each attempt sees fresh data.
 *
 * The final attempt runs without catching, so Playwright reports the full
 * assertion diff rather than a generic timeout message.
 *
 * @example
 * await retryOnFail(
 *   async () => {
 *     await expect(page.getByTestId('tag-container')).toContainText('MyTag');
 *   },
 *   {
 *     onRetry: async () => {
 *       await page.reload();
 *       await page.waitForLoadState('networkidle');
 *     },
 *   },
 * );
 */
export async function retryOnFail(
  assertions: () => Promise<void>,
  { onRetry, timeout = DEFAULT_TIMEOUT, interval = POLL_INTERVAL }: RetryOnFailOptions = {},
): Promise<void> {
  const deadline = Date.now() + timeout;

  while (Date.now() < deadline) {
    try {
      await assertions();
      return;
    } catch {
      await onRetry?.();
      await new Promise<void>((resolve) => setTimeout(resolve, interval));
    }
  }

  await assertions();
}
