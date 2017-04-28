const urnRegex = /^([a-z_]+):\/{3}([a-z_\-\/]*)/i;
/**
 * Asserts that a provided string matches the urn pattern above
 * @param {String} candidateUrn the string to test on
 */
export default candidateUrn => urnRegex.test(String(candidateUrn));

export { urnRegex };
