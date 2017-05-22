/**
 * A list of id logical types
 * @type {Array.<String>}
 */
const idLogicalTypes = ['ID', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'];

/**
 * Default mapping of field data types to security classification
 * https://iwww.corp.linkedin.com/wiki/cf/display/DWH/List+of+Metadata+for+Data+Sets
 * @type {{MEMBER_ID: string, MEMBER_URN: string, MEMBER_REVERSE_URN: string, COMPOSITE_URN: string, COMPANY_ID: string, COMPANY_URN: string, GROUP_ID: string, GROUP_URN: string, NAME: string, EMAIL: string, PHONE_NUMBER: string, PHONE_URN: string, ADDRESS: string, GEO_LOCATION: string, IP_ADDRESS: string, FINANCIAL_NUMBER: string, PAYMENT_INFO: string, PASSWORD_AND_CREDENTIALS: string, AUTHENTICATION_TOKEN: string, MESSAGE: string, NATIONAL_ID: string, SOCIAL_NETWORK_ID: string, EVENT_TIME: string, TRANSACTION_TIME: string, COOKIES_AND_BEACONS_AND_BROWSER_ID: string, DEVICE_ID_AND_ADVERTISING_ID: string}}
 */
const nonIdFieldDataTypeClassification = {
  NAME: 'confidential',
  EMAIL: 'confidential',
  PHONE_NUMBER: 'confidential',
  ADDRESS: 'confidential',
  LATITUDE_AND_LONGITUDE: 'confidential',
  'CITY_/STATE_/REGION_ETC': 'limitedDistribution',
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
 * List of non Id field data type classifications
 * @type {Array}
 */
const genericLogicalTypes = Object.keys(nonIdFieldDataTypeClassification).sort();

/**
 * A map of id logical types to the default field classification for Ids
 * @type {Object}
 */
const idFieldDataTypeClassification = idLogicalTypes.reduce((classification, idLogicalType) => {
  return Object.assign({}, classification, { [idLogicalType]: 'limitedDistribution' });
}, {});

/**
 * A merge of id and non id field type classifications
 * @type {Object}
 */
const defaultFieldDataTypeClassification = Object.assign(
  {},
  idFieldDataTypeClassification,
  nonIdFieldDataTypeClassification
);

/**
 * Stores a unique list of classification values
 * @type {Array.<String>} the list of classification values
 */
const classifiers = Object.values(defaultFieldDataTypeClassification).filter(
  (classifier, index, iter) => iter.indexOf(classifier) === index
);

/**
 * A map of identifier types for fields on a dataset
 * @type {{none: {value: string, isId: boolean, displayAs: string}, member: {value: string, isId: boolean, displayAs: string}, subjectMember: {value: string, isId: boolean, displayAs: string}, group: {value: string, isId: boolean, displayAs: string}, organization: {value: string, isId: boolean, displayAs: string}, generic: {value: string, isId: boolean, displayAs: string}}}
 */
const fieldIdentifierTypes = {
  none: {
    value: 'none',
    isId: false,
    displayAs: 'None'
  },
  member: {
    value: 'member',
    isId: true,
    displayAs: 'Member ID'
  },
  subjectMember: {
    value: 'subjectMember',
    isId: true,
    displayAs: 'Member ID (Subject Owner)'
  },
  group: {
    value: 'group',
    isId: true,
    displayAs: 'Group ID'
  },
  organization: {
    value: 'organization',
    isId: true,
    displayAs: 'Organization ID'
  },
  generic: {
    value: 'mixed',
    isId: false,
    displayAs: 'Mixed'
  }
};

export { defaultFieldDataTypeClassification, classifiers, fieldIdentifierTypes, idLogicalTypes, genericLogicalTypes };
