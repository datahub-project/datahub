import { identity, not } from 'wherehows-web/utils/helpers/functions';

/**
 * Composable function that will in turn consume an item from a list an emit a result of equal or same type
 * @type Iteratee
 */
type Iteratee<A, R> = (a: A) => R;

/**
 * Aliases a type T or an array of type T
 * @template T
 * @alias
 */
type Many<T> = T | Array<T>;

/**
 * Takes a number of elements in the list from the start up to the length of the list
 * @template T type of elements in array
 * @param {number} [n=0] number of elements to take from the start of the array
 */
const take = <T>(n: number = 0) => (list: Array<T>): Array<T> => Array.prototype.slice.call(list, 0, n < 0 ? 0 : n);

/**
 * Convenience utility takes a type-safe mapping function, and returns a list mapping function
 * @param {(param: T) => U} predicate maps a single type T to type U
 * @return {(array: Array<T>) => Array<U>}
 */
const arrayMap = <T, U>(predicate: (param: T) => U): ((array: Array<T>) => Array<U>) => (array = []) =>
  array.map(predicate);

/**
 * Partitions an array into a tuple containing elements that meet the predicate in the zeroth index,
 * and excluded elements in the next
 * `iterate-first data-last` function
 * @template T type of source element list
 * @template U subtype of T in first partition
 * @param {(param: T) => param is U} predicate is a type guard function
 * @returns {((array: Array<T>) => [Array<U>, Array<Exclude<T, U>>])}
 */
const arrayPartition = <T, U extends T>(
  predicate: (param: T) => param is U
): ((array: Array<T>) => [Array<U>, Array<Exclude<T, U>>]) => (array = []) => [
  array.filter(predicate),
  array.filter<Exclude<T, U>>((v: T): v is Exclude<T, U> => not(predicate)(v))
];

/**
 * Convenience utility takes a type-safe filter function, and returns a list filtering function
 * @param {(param: T) => boolean} predicate
 * @return {(array: Array<T>) => Array<T>}
 */
const arrayFilter = <T>(predicate: (param: T) => boolean): ((array: Array<T>) => Array<T>) => (array = []) =>
  array.filter(predicate);

/**
 * Type safe utility `iterate-first data-last` function for array every
 * @template T
 * @param {(param: T) => boolean} predicate
 * @returns {((array: Array<T>) => boolean)}
 */
const arrayEvery = <T>(predicate: (param: T) => boolean): ((array: Array<T>) => boolean) => (array = []) =>
  array.every(predicate);

/**
 * Type safe utility `iterate-first data-last` function for array some
 * @template T
 * @param {(param: T) => boolean} predicate
 * @return {(array: Array<T>) => boolean}
 */
const arraySome = <T>(predicate: (param: T) => boolean): ((array: Array<T>) => boolean) => (array = []) =>
  array.some(predicate);

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

// arrayPipe overloads
function arrayPipe<T, R1 = T>(f1: (a1: T) => R1): (x: T) => R1;
function arrayPipe<T, R1 = T, R2 = T>(f1: (a1: T) => R1, f2: (a2: R1) => R2): (x: T) => R2;
function arrayPipe<T, R1 = T, R2 = T, R3 = T>(f1: (a1: T) => R1, f2: (a2: R1) => R2, f3: (a3: R2) => R3): (x: T) => R3;
function arrayPipe<T, R1 = T, R2 = T, R3 = T, R4 = T>(
  f1: (a1: T) => R1,
  f2: (a2: R1) => R2,
  f3: (a3: R2) => R3,
  f4: (a4: R3) => R4
): (x: T) => R4;
function arrayPipe<T, R1 = T, R2 = T, R3 = T, R4 = T, R5 = T>(
  f1: (a1: T) => R1,
  f2: (a2: R1) => R2,
  f3: (a3: R2) => R3,
  f4: (a4: R3) => R4,
  f5: (a5: R4) => R5
): (x: T) => R5;
function arrayPipe<T, R1, R2, R3, R4, R5, R6>(
  f1: (a1: T) => R1,
  f2: (a2: R1) => R2,
  f3: (a3: R2) => R3,
  f4: (a4: R3) => R4,
  f5: (a5: R4) => R5,
  f6: (a6: R5) => R6
): (x: T) => R6;
function arrayPipe<T, R1, R2, R3, R4, R5, R6, R7>(
  f1: (a1: T) => R1,
  f2: (a2: R1) => R2,
  f3: (a3: R2) => R3,
  f4: (a4: R3) => R4,
  f5: (a5: R4) => R5,
  f6: (a6: R5) => R6,
  f7: (a7: R6) => R7
): (x: T) => R7;
/**
 * overload to handle case of too many functions being piped, provides less type safety once args exceeds 7 iteratees
 * @param {(a1: T) => R1} f1
 * @param {(a2: R1) => R2} f2
 * @param {(a3: R2) => R3} f3
 * @param {(a4: R3) => R4} f4
 * @param {(a5: R4) => R5} f5
 * @param {(a6: R5) => R6} f6
 * @param {(a7: R6) => R7} f7
 * @param {Many<Iteratee<any, any>>} fns
 * @return {(arg: T) => any}
 */
function arrayPipe<T, R1, R2, R3, R4, R5, R6, R7>(
  f1: (a1: T) => R1,
  f2: (a2: R1) => R2,
  f3: (a3: R2) => R3,
  f4: (a4: R3) => R4,
  f5: (a5: R4) => R5,
  f6: (a6: R5) => R6,
  f7: (a7: R6) => R7,
  ...fns: Array<Many<Iteratee<any, any>>>
): (arg: T) => any;

/**
 * Takes a list (array / separate args) of iteratee functions, with each successive iteratee is
 * invoked with the result of the previous iteratee invocation
 * @template T the type of elements in the array
 * @template R the result of executing the last iteratee
 * @param {Many<Iteratee<any, any>>} fns
 * @return {(x: T) => R}
 */
function arrayPipe<T, R>(...fns: Array<Many<Iteratee<any, any>>>): (x: T) => R {
  return arrayReduce<(a: T) => any, (x: any) => R>((acc, f) => (x): R => acc(f(x)), identity)(
    (<Array<Iteratee<any, any>>>[]).concat(...fns.reverse()) // flatten if arg is of type Array<>
  );
}

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

// type exports, might need to move to a declaration file*
export { Many, Iteratee };

export {
  take,
  arrayMap,
  arrayPipe,
  arrayFilter,
  arrayReduce,
  arrayPartition,
  isListUnique,
  compact,
  arrayEvery,
  arraySome,
  iterateArrayAsync,
  reduceArrayAsync
};
