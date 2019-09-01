/**
 * String pattern for a standardized platform urn for a dataset
 * @type {string}
 */
export const datasetPlatformUrnPattern = `urn:li:dataPlatform:([\\w-]+)`;

/**
 * Matches a string that follows the pattern defined in datasetPlatformUrnPattern
 * @type {RegExp}
 */
export const datasetPlatformRegExp = new RegExp(datasetPlatformUrnPattern);
