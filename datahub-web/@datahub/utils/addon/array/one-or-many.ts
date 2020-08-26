/**
 * Given some value as an argument, if that value is a single object or primitive then return it
 * as an array of that object or primitive, if that value is an array then return it as an array.
 * Guarantees that whatever comes from this function will be an array
 * Note: This function does not support an array that is intended to be an array of arrays
 * @param {T | Array<T>} value - some value that may be in array form or as a single item
 */
export const oneOrMany = <T>(value: T | Array<T>): Array<T> => (Array.isArray(value) ? value : [value]);
