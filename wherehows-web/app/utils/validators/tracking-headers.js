// Matches field identifiers starting with header, requestHeader, mobileHeader,
//   mobile_header, request_header
//   all optionally suffixed with a period and any non - line break characters
const trackingHeaderFieldsRegex = /^(?:header\.?|request_?header\.?|mobile_?header\.?).*/i;

export default candidateString => trackingHeaderFieldsRegex.test(String(candidateString));

export { trackingHeaderFieldsRegex };
