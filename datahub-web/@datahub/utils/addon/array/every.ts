/**
 * Type safe utility `iterate-first data-last` function for array every
 * @template T
 * @param {(param: T) => boolean} predicate
 * @returns {((array: Array<T>) => boolean)}
 */
export const arrayEvery = <T>(predicate: (param: T) => boolean): ((array: Array<T>) => boolean) => (array = []) =>
  array.every(predicate);
