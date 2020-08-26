/**
 * Generates a list of random floating point numbers between 0 and 100 exclusive
 * @param {number} length the number of numbers to generate
 * @return {Array<number>} the list of random numbers
 */
const xRandomNumbers = (length: number): Array<number> => Array.from(new Array(length)).map(() => Math.random() * 100);

/**
 * Converts a number to a string
 * @param {number} num the number to convert
 * @return {string} the number as a string
 */
const numToString = (num: number): string => num + '';

/**
 * Checks if a value is a string
 * @param {unknown} param the value to check
 * @return {boolean} true, if the value is a string
 */
const isAString = (param: unknown): param is string => typeof param === 'string';

export { xRandomNumbers, numToString, isAString };
