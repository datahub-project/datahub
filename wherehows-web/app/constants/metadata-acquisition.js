/**
 * Default mapping of field data types to security classification
 * https://iwww.corp.linkedin.com/wiki/cf/display/DWH/List+of+Metadata+for+Data+Sets
 * @type {{MEMBER_ID: string, MEMBER_URN: string, MEMBER_REVERSE_URN: string, COMPOSITE_URN: string, COMPANY_ID: string, COMPANY_URN: string, GROUP_ID: string, GROUP_URN: string, NAME: string, EMAIL: string, PHONE_NUMBER: string, PHONE_URN: string, ADDRESS: string, GEO_LOCATION: string, IP_ADDRESS: string, FINANCIAL_NUMBER: string, PAYMENT_INFO: string, PASSWORD_AND_CREDENTIALS: string, AUTHENTICATION_TOKEN: string, MESSAGE: string, NATIONAL_ID: string, SOCIAL_NETWORK_ID: string, EVENT_TIME: string, TRANSACTION_TIME: string, COOKIES_AND_BEACONS_AND_BROWSER_ID: string, DEVICE_ID_AND_ADVERTISING_ID: string}}
 */
const defaultFieldDataTypeClassification = {
  NAME: 'confidential',
  EMAIL: 'confidential',
  PHONE_NUMBER: 'confidential',
  PHONE_URN: 'confidential',
  ADDRESS: 'confidential',
  GEO_LOCATION: 'confidential',
  IP_ADDRESS: 'confidential',
  FINANCIAL_NUMBER: 'confidential',
  PAYMENT_INFO: 'highlyConfidential',
  PASSWORD_AND_CREDENTIALS: 'highlyConfidential',
  AUTHENTICATION_TOKEN: 'highlyConfidential',
  MESSAGE: 'highlyConfidential',
  NATIONAL_ID: 'highlyConfidential',
  SOCIAL_NETWORK_ID: 'confidential',
  EVENT_TIME: 'limitedDistribution',
  TRANSACTION_TIME: 'limitedDistribution',
  COOKIES_AND_BEACONS_AND_BROWSER_ID: 'confidential',
  DEVICE_ID_AND_ADVERTISING_ID: 'confidential'
};

/**
 * Stores a unique list of classification values
 * @type {Array.<String>} the list of classification values
 */
const classifiers = Object.values(defaultFieldDataTypeClassification).filter(
  (classifier, index, iter) => iter.indexOf(classifier) === index
);

export { defaultFieldDataTypeClassification, classifiers };
