import { helper } from '@ember/component/helper';

/**
 * Turns a timestamp given in ms to a unix time (as seconds)
 * @param {number | string} timestamp - the given timestamp in ms
 */
export function msTimeAsUnix([timestamp]: [number | string]): number {
  if (typeof timestamp === 'string') {
    timestamp = parseInt(timestamp);
  }

  return Math.ceil(timestamp / 1000);
}

export default helper(msTimeAsUnix);
