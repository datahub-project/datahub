const headers = ['header.', 'header[field]', 'header.field', 'header[]'];
const requestHeaders = ['requestheader[]', 'requestheader.', 'request_header.mockId', 'request_header.s[]'];
const mobileHeaders = [
  'mobileheader.',
  'MobileHeader[]',
  'mobile_header.header',
  'mobileheader.header',
  'mobileheader.string'
];

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
  'header',
  'headers',
  'Header',
  'headerfortrackingisinvalid',
  'HEADER_PATH',
  'HEADER_REFERER',
  'HEADER_USERAGENT',
  '',
  '-',
  'sub-header',
  'identity-requested',
  'dataMobility',
  'mobile phone',
  'heading',
  'requests',
  'request_headers',
  'mobile_header-string'
];

export { trackingHeaderList, nonTrackingHeaderList };
