/**
 * Aliases an element in Array type T
 * @template T where T is assignable to ArrayLike<unknown>
 * @type ArrayElement<T>
 */
export type ArrayElement<T extends ArrayLike<unknown>> = T[0];
