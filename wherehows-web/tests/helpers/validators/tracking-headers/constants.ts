const headers = ['header', 'header.', 'header_', 'headers', 'headerfortracking', 'Header'];
const requestHeaders = ['requestheader', 'requestheader.', 'request_header.mockId', 'request_headers'];
const mobileHeaders = ['mobileheader', 'MobileHeader', 'mobile_header', 'mobile_header-string'];

/**
 * Collects an array of strings identified as valid tracking headers
 * @type {Array<string>}
 */
const trackingHeaderList = [...headers, ...requestHeaders, ...mobileHeaders];

/**
 * A list of strings identified as non tracking headers
 * @type {Array<string>}
 */
const nonTrackingHeaderList = [
  'sub-header',
  'identity-requested',
  'dataMobility',
  'mobile phone',
  'heading',
  'requests'
];

export { trackingHeaderList, nonTrackingHeaderList };
