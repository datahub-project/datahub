/**
 *  Matches field identifiers starting with header, requestHeader, mobileHeader,
 *  mobile_header, request_header
 *  or solely those strings
 *  optionally suffixed with a period and any non - line break characters
 */
const trackingHeaderFieldsRegex = /^(?:header$|header[.|[]|request_?header$|request_?header[.|[]|mobile_?header$|mobile_?header[.|[]).*/i;

/**
 * Tests if the supplied string matches any of the tracking header regex'
 * @param {string} candidateString
 * @return {boolean}
 */
export default (candidateString: string): boolean => trackingHeaderFieldsRegex.test(String(candidateString));

export { trackingHeaderFieldsRegex };
