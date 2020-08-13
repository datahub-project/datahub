/**
 * Checks if a type is an object
 * @param {any} candidate the entity to check
 */
export const isObject = (candidate: unknown): candidate is object =>
  candidate !== null && typeof candidate === 'object';
