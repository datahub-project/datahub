/**
 * Matches a url string with a `urn` query. urn query with letters or underscore segment of any length greater
 *   than 1 followed by colon and 3 forward slashes and a segment containing letters, {, }, _ or /, or none
 *   The value following the urn key is retained
 * @type {RegExp}
 */
const datasetUrnRegexWH = /([a-z_]+):\/{3}([a-z0-9_\-/{}]*)/i;

/**
 * Matches a urn string that follows the pattern captures, the comma delimited platform, segment and fabric
 * e.g urn:li:dataset:(urn:li:dataPlatform:PLATFORM,SEGMENT,FABRIC)
 * @type {RegExp}
 */
const datasetUrnRegexLI = /urn:li:dataset:\(urn:li:dataPlatform:(\w+),([\w.\-]+),(\w+)\)/;

/**
 * Matches urn's that occur in flow urls
 * @type {RegExp}
 */
const specialFlowUrnRegex = /(?:\?urn=)([a-z0-9_\-/{}\s]+)/i;

/**
 * Checks if a string matches the datasetUrnRegexWH
 * @param {string} candidateUrn
 * @returns {boolean}
 */
const isWhUrn = (candidateUrn: string): boolean => datasetUrnRegexWH.test(String(candidateUrn));

/**
 * Checks if a string matches the datasetUrnRegexLI
 * @param {string} candidateUrn
 * @returns {boolean}
 */
const isLiUrn = (candidateUrn: string): boolean => datasetUrnRegexLI.test(String(candidateUrn));

/**
 * Asserts that a provided string matches the urn pattern above
 * @param {string} candidateUrn the string to test on
 */
const isUrn = (candidateUrn: string) => isLiUrn(candidateUrn) || isWhUrn(candidateUrn);

/**
 * Extracts the platform string from the candidate urn string
 * @param {string} candidateUrn the urn string with leading platform identifier
 * @returns {string | void}
 */
const getPlatformFromUrn = (candidateUrn: string) => {
  const matches = datasetUrnRegexWH.exec(candidateUrn);

  if (matches) {
    const [, platform] = matches;
    return platform.toUpperCase();
  }
};

export default isUrn;

export { datasetUrnRegexWH, datasetUrnRegexLI, isWhUrn, isLiUrn, specialFlowUrnRegex, getPlatformFromUrn };
