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
 * Composable reducer abstraction, curries a reducing iteratee and returns a reducing function that takes a list
 * @template U
 * @param {(acc: U) => U} iteratee
 * @param {U} init the initial value in the reduction sequence
 * @return {(arr: Array<T>) => U}
 */
const arrayReduce = <T, U>(
  iteratee: (accumulator: U, element: T, index: number, collection: Array<T>) => U,
  init: U
): ((arr: Array<T>) => U) => (array = []) => array.reduce(iteratee, init);

/**
 * Duplicate check using every to short-circuit iteration
 * @template T
 * @param {Array<T>} [list = []] list to check for dupes
 * @return {boolean} true is unique
 */
const isListUnique = <T>(list: Array<T> = []): boolean => new Set(list).size === list.length;

/**
 * Extracts all non falsey values from a list.
 * @template T
 * @param {Array<T>} list the list of items to compact
 * @return {Array<T>}
 */
const compact = <T>(list: Array<T> = []): Array<T> => list.filter(item => item);

export { arrayMap, arrayFilter, arrayReduce, isListUnique, compact };
