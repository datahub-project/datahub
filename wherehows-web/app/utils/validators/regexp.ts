/**
 * Constant value for an empty regex source string
 * @type {string}
 */
const emptyRegexSource = '(?:)';

/**
 * Sanitizes a string to be used in creating a runtime regular expression pattern by escaping special characters
 * @param {string} pattern the string intended to be used to new a RegExp object
 * @returns {string}
 */
const sanitizeRegExp = (pattern: string): string => pattern.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');

/**
 * Creates a new RegExp object using the pattern argument
 * @param {string} pattern the string to build the regular expression with
 * @param {string} [flags] flags to be passed to the RegExp constructor
 * @returns {RegExp}
 */
const buildSaneRegExp = (pattern: string, flags?: string): RegExp => new RegExp(sanitizeRegExp(pattern), flags);

export default buildSaneRegExp;

export { sanitizeRegExp, buildSaneRegExp, emptyRegexSource };
