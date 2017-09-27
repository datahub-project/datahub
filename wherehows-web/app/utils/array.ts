/**
 * Convenience utility takes a type-safe mapping function, and returns a list mapping function
 * @param {(param: T) => U} mappingFunction maps a single type T to type U
 * @return {(array: Array<T>) => Array<U>}
 */
const arrayMap = <T, U>(mappingFunction: (param: T) => U): ((array: Array<T>) => Array<U>) => (array = []) =>
  array.map(mappingFunction);

/**
 * Convenience utility takes a type-safe filter function, and returns a list filtering function
 * @param {(param: T) => boolean} filtrationFunction
 * @return {(array: Array<T>) => Array<T>}
 */
const arrayFilter = <T>(filtrationFunction: (param: T) => boolean): ((array: Array<T>) => Array<T>) => (array = []) =>
  array.filter(filtrationFunction);

/**
 * Duplicate check using every to short-circuit iteration
 * @param {Array<T>} [list = []] list to check for dupes
 * @return {boolean} true is unique
 */
const isListUnique = <T>(list: Array<T> = []): boolean => new Set(list).size === list.length;

export { arrayMap, arrayFilter, isListUnique };
