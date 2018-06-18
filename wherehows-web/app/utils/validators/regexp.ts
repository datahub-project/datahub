/**
 * Constant value for an empty regex source string
 * @type {string}
 */
const emptyRegexSource = '(?:)';

/**
 * Checks that the pattern parameter is a valid RegExp string and optionally checks with a customChecker as well
 * @param {string} pattern the string to validate
 * @param {(pattern: string) => boolean} [customChecker] an optional function to check the pattern against
 * @return {boolean}
 * @throws SyntaxError
 */
const validateRegExp = (
  pattern: string | null | undefined,
  customChecker?: (pattern: any) => boolean
): { isValid: boolean; regExp: RegExp } => {
  const regExp = new RegExp(pattern!);
  const { source } = regExp;
  const isValid = !!pattern && source !== emptyRegexSource;

  if (isValid && customChecker && customChecker(pattern)) {
    return { isValid, regExp };
  }

  return { isValid, regExp };
};

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

export { sanitizeRegExp, buildSaneRegExp, emptyRegexSource, validateRegExp };
