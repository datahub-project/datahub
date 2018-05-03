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
 * Type safe utility `iterate-first data-last` function for array every
 * @template T
 * @param {(param: T) => boolean} filter
 * @returns {((array: Array<T>) => boolean)}
 */
const arrayEvery = <T>(filter: (param: T) => boolean): ((array: Array<T>) => boolean) => (array = []) =>
  array.every(filter);

/**
 * Type safe utility `iterate-first data-last` function for array some
 * @template T
 * @param {(param: T) => boolean} filter
 * @return {(array: Array<T>) => boolean}
 */
const arraySome = <T>(filter: (param: T) => boolean): ((array: Array<T>) => boolean) => (array = []) =>
  array.some(filter);

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

/**
 * Defines the interface for options that may be passed into the chunk function
 * @interface {IChunkArrayOptions}
 */
interface IChunkArrayOptions {
  chunkSize?: 50 | 100;
  context?: null | object;
}

/**
 * Asynchronously traverses a list in small chunks ensuring that a list can be iterated over without
 * blocking the browser main thread.
 * @template T type of values in list to be iterated over
 * @template U the type of the value that is produced by an iteration of the list
 * @param {(arr?: Array<T>) => U} iterateeSync an iteratee that consumes an list and returns a value of type U
 * @param {(res: U) => U} accumulator a function that combines the result of successive iterations of the original list
 * @param {IChunkArrayOptions} [{chunkSize = 50, context = null}={chunkSize: 50, context: null}]
 * @param {50 | 100} chunkSize the maximum size to chunk at a time
 * @param {object | null} [context] the optional execution context for the iteratee invocation
 * @return {(list: Array<T>) => Promise<U>}
 */
const chunkArrayAsync = <T, U>(
  iterateeSync: (arr?: Array<T>) => U,
  accumulator: (res: U) => U,
  { chunkSize = 50, context = null }: IChunkArrayOptions = { chunkSize: 50, context: null }
) => (list: Array<T>) =>
  new Promise<U>(function(resolve) {
    const queue = list.slice(0); // creates a shallow copy of the original list
    let result: U;

    requestAnimationFrame(function chunk() {
      const startTime = +new Date();

      do {
        result = accumulator(iterateeSync.call(context, queue.splice(0, chunkSize)));
      } while (queue.length && +new Date() + startTime < 18);

      // recurse through list if there are more items left
      return queue.length ? requestAnimationFrame(chunk) : resolve(result);
    });
  });

/**
 * Asynchronously traverse a list and accumulate another list based on the iteratee
 * @template T the type of values in the original list
 * @template U the type of values in the transformed list
 * @param {(arr?: Array<T>) => Array<U>} iteratee consumes a list and returns a new list of values
 * @return {(list: Array<T>, context?: any) => Promise<Array<U>>}
 */
const iterateArrayAsync = <T, U = T>(iteratee: (arr?: Array<T>) => Array<U>) => (
  list: Array<T>,
  context = null
): Promise<Array<U>> => {
  const accumulator = (base: Array<U>) => (arr: Array<U>) => (base = [...base, ...arr]);
  return chunkArrayAsync(iteratee, accumulator([]), { chunkSize: 50, context })(list);
};

/**
 * Asynchronously traverse a list and accumulate a value of type U, applies to cases of reduction or accumulation
 * @template T the type of values in the original list
 * @template U the type of value to be produced by reducing the list
 * @param {(arr?: Array<T>) => U} reducer consumes a list and produces a single value
 * @return {(list: Array<T>, context?: any) => Promise<U>}
 */
const reduceArrayAsync = <T, U>(reducer: (arr?: Array<T>) => U) => (list: Array<T>, context = null): Promise<U> => {
  const accumulator = (base: U) => (int: U) => Object.assign(base, int);
  return chunkArrayAsync(reducer, accumulator(reducer.call(context)))(list);
};

export {
  arrayMap,
  arrayFilter,
  arrayReduce,
  isListUnique,
  compact,
  arrayEvery,
  arraySome,
  iterateArrayAsync,
  reduceArrayAsync
};
