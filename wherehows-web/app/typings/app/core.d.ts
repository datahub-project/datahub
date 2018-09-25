/**
 * Defines a generic type for a type T that could also be undefined
 * @template T type which maybe is generic over
 */
type Maybe<T> = T | undefined;

/**
 * Defines a generic type that recasts a type T as a union of U and an intersection of T and U
 * @template T the type to be recast
 * @template U the result of the type recast
 */
type Recast<T, U> = T & U | U;

export { Maybe, Recast };
