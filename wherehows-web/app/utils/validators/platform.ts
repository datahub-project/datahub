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

/**
 * Takes a string and strips out the platform metadata and returns the sanitized string if it exists,
 * otherwise returns the original string
 * @param {string} nodeString
 * @returns {string}
 */
const sanitizePlatformNodeString = (nodeString: string): string => {
  if (isDatasetPlatform(nodeString)) {
    const platform = getPlatformFromString(nodeString);

    return platform ? platform : nodeString;
  }

  return nodeString;
};

export { platformRegex, isDatasetPlatform, isDatasetSegment, getPlatformFromString, sanitizePlatformNodeString };
