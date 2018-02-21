/**
 * Matches a string that is prefixed by [platform and suffixed by ]
 * Captures the match in the sole capture group
 * @type {RegExp}
 */
const platformRegex = /\[platform=([^]]*)]/;

/**
 * Checks that a string represents a dataset platform
 * @param {string} candidate
 * @returns {boolean}
 */
const isDatasetPlatform = (candidate: string): boolean => platformRegex.test(candidate);

/**
 * Checks that a string represents a dataset prefix
 * @param {string} candidate
 * @returns {boolean}
 */
const isDatasetPrefix = (candidate: string): boolean =>
  !isDatasetPlatform(candidate) && ['.', '/'].includes(candidate.slice(-1));

export { platformRegex, isDatasetPlatform, isDatasetPrefix };
