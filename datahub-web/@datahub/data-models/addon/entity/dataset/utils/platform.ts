/**
 * Matches a string that is prefixed by [platform and suffixed by ]
 * Captures the match in the sole capture group
 * @type {RegExp}
 */
const platformRegex = /\[platform=([^\]]+)]/;

/**
 * Checks that a string represents a dataset platform
 * @param {string} candidate
 * @returns {boolean}
 */
const isDatasetPlatform = (candidate: string): boolean => platformRegex.test(candidate);

/**
 * Checks that a string represents a dataset segment
 * @param {string} candidate
 * @returns {boolean}
 */
const isDatasetSegment = (candidate: string): boolean =>
  !isDatasetPlatform(candidate) && ['.', '/'].includes(candidate.slice(-1));

/**
 * Checks that a string is not a dataset platform or segment
 * @param {string} candidate
 * @return {boolean}
 */
const isDatasetIdentifier = (candidate: string): boolean =>
  !!candidate && !isDatasetPlatform(candidate) && !isDatasetSegment(candidate); // not a platform and does not meet segment rules

/**
 * Takes an encoded platform string and strips out the coding metadata
 * @param {string} candidateString
 * @returns {(string | void)}
 */
const getPlatformFromString = (candidateString: string): string | void => {
  const resultArray: Array<string> | null = platformRegex.exec(candidateString);

  if (resultArray) {
    const [, platform] = resultArray;
    return platform;
  }
};

export { platformRegex, isDatasetPlatform, isDatasetSegment, getPlatformFromString, isDatasetIdentifier };
