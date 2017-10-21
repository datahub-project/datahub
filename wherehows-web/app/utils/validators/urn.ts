/**
 * Matches a url string with a `urn` query. urn query with letters or underscore segment of any length greater
 *   than 1 followed by colon and 3 forward slashes and a segment containing letters, {, }, _ or /, or none
 *   The value following the urn key is retained
 * @type {RegExp}
 */
const urnRegex = /([a-z_]+):\/{3}([a-z0-9_\-/{}]*)/i;

/**
 * Matches urn's that occur in flow urls
 * @type {RegExp}
 */
const specialFlowUrnRegex = /(?:\?urn=)([a-z0-9_\-/{}\s]+)/i;

/**
 * Asserts that a provided string matches the urn pattern above
 * @param {string} candidateUrn the string to test on
 */
const isUrn = (candidateUrn: string) => urnRegex.test(String(candidateUrn));

/**
 * Extracts the platform string from the candidate urn string
 * @param {string} candidateUrn the urn string with leading platform identifier
 * @returns {string | void}
 */
const getPlatformFromUrn = (candidateUrn: string) => {
  const matches = urnRegex.exec(candidateUrn);
  if (matches) {
    const [, platform] = matches;
    return platform.toUpperCase();
  }
};

export default isUrn;

export { urnRegex, specialFlowUrnRegex, getPlatformFromUrn };
