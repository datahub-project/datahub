import { waitUntil, getSettledState } from '@ember/test-helpers';

/**
 * Check for rendering state settlement i.e. excluding pending timers
 */
export const renderingDone = (): Promise<boolean> =>
  waitUntil((): boolean => {
    const { hasPendingRequests, hasPendingWaiters, hasRunLoop } = getSettledState();
    return !(hasPendingRequests || hasPendingWaiters || hasRunLoop);
  });
