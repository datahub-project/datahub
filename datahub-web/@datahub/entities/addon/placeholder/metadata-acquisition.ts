// TODO: [META-8255] These are placeholder utils that exist here until we find them a home in the proper location.

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
export const validateRegExp = (
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
 * Checks that a string matches the expected valuePatternRegex
 * @param {string} candidate the supplied pattern string
 * @return {boolean}
 */
export const isValidCustomValuePattern = (candidate: string): boolean => !!candidate;
