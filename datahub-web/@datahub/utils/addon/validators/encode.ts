/**
 * Returns a value with all periods encoded with '%2E' as this helps in many ember situations where
 * trying to run .get() or .set() on a value with periods will run into errors
 * @param value - passed in string
 */
export const encodeDot = (value: string): string => value.replace(/[.]/g, '%2E');

/**
 * Returns a value that has been encoded for dots to be decoded back to its original form
 * @param value - passed in encoded string
 */
export const decodeDot = (value: string): string => value.replace(/%2E/g, '.');
