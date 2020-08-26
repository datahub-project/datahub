/**
 * Negates a boolean function
 * @param {(arg: T) => boolean} fn the boolean function to negate
 * @return {(arg: T) => boolean} curried function that will receive the arg to the boolean function
 */
export const not = <T>(fn: (arg: T) => boolean) => (arg: T) => !fn(arg);

/**
 * Identity function, immediately returns its argument
 * @template T
 * @param {T} x
 * @return {T}
 */
export const identity = <T>(x: T): T => x;
