/**
 * Matches a url string with a `urn` query. urn query with letters or underscore segment of any length greater
 *   than 1 followed by colon and 3 forward slashes and a segment containing letters, {, }, _ or /, or none
 *   The value following the urn key is retained
 * @type {RegExp}
 */
const urnRegex = /([a-z_]+):\/{3}([a-z0-9_\-\/\{\}]*)/i;

/**
 * Matches urn's that occur in flow urls
 * @type {RegExp}
 */
const specialFlowUrnRegex = /(?:\?urn=)([a-z0-9_\-\/{}\s]+)/i;
/**
 * Asserts that a provided string matches the urn pattern above
 * @param {String} candidateUrn the string to test on
 */
export default candidateUrn => urnRegex.test(String(candidateUrn));

export { urnRegex, specialFlowUrnRegex };
