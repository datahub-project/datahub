/**
 * Waits a given number of seconds before resolving
 * @param {number} seconds the duration to wait for
 * @return {Promise<void>}
 */
export const delay = (seconds = 5): Promise<void> => new Promise(resolve => setTimeout(resolve, seconds * 1000));
