import { later } from '@ember/runloop';

/**
 * Helper to convert a timeout into a promise which is more convinient for some places
 * @param ms milliseconds that you need to wait
 */
export function waitTime(ms: number): Promise<void> {
  const promise: Promise<void> = new Promise((resolve): void => {
    later((): void => resolve(), ms);
  });
  return promise;
}
