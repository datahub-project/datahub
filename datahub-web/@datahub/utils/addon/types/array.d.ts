/**
 * Aliases an element in Array type T
 * @template T where T is assignable to ArrayLike<unknown>
 * @type ArrayElement<T>
 */
export type ArrayElement<T extends ArrayLike<unknown>> = T[0];

/**
 * Aliases a type T or an array of type T
 * @template T
 * @alias
 */
export type Many<T> = T | Array<T>;

/**
 * Composable function that will in turn consume an item from a list an emit a result of equal or same type
 * @type Iteratee
 */
export type Iteratee<A, R> = (a: A) => R;
