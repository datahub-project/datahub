/**
 * A list of id logical types
 * @type {Array.<String>}
 */
const idLogicalTypes = ['ID', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'];

// Default mapping of field data types to security classification
// https://iwww.corp.linkedin.com/wiki/cf/display/DWH/List+of+Metadata+for+Data+Sets
const nonIdFieldLogicalTypes = {
  NAME: {
    classification: 'confidential',
    displayAs: 'Name'
  },
  EMAIL: {
    classification: 'confidential',
    displayAs: 'E-mail'
  },
  PHONE_NUMBER: {
    classification: 'confidential',
    displayAs: 'Phone Number'
  },
  ADDRESS: {
    classification: 'confidential',
    displayAs: 'Address'
  },
  LATITUDE_AND_LONGITUDE: {
    classification: 'confidential',
    displayAs: 'Latitude and Longitude'
  },
  'CITY_/STATE_/REGION_ETC': {
    classification: 'limitedDistribution',
    displayAs: 'City, State, Region, etcetera'
  },
  IP_ADDRESS: {
    classification: 'confidential',
    displayAs: 'IP Address'
  },
  FINANCIAL_NUMBER: {
    classification: 'confidential',
    displayAs: 'Financial Number'
  },
  PAYMENT_INFO: {
    classification: 'highlyConfidential',
    displayAs: 'Payment Info'
  },
  PASSWORD_AND_CREDENTIALS: {
    classification: 'highlyConfidential',
    displayAs: 'Password and Credentials'
  },
  AUTHENTICATION_TOKEN: {
    classification: 'highlyConfidential',
    displayAs: 'Authentication Token'
  },
  MESSAGE: {
    classification: 'highlyConfidential',
    displayAs: 'Message'
  },
  NATIONAL_ID: {
    classification: 'highlyConfidential',
    displayAs: 'National Id'
  },
  SOCIAL_NETWORK_ID: {
    classification: 'confidential',
    displayAs: 'Social Network Id'
  },
  EVENT_TIME: {
    classification: 'limitedDistribution',
    displayAs: 'Event Time'
  },
  TRANSACTION_TIME: {
    classification: 'limitedDistribution',
    displayAs: 'Transaction Time'
  },
  COOKIES_AND_BEACONS_AND_BROWSER_ID: {
    classification: 'confidential',
    displayAs: 'Cookies and Beacons and Browser Id'
  },
  DEVICE_ID_AND_ADVERTISING_ID: {
    classification: 'confidential',
    displayAs: 'Device Id and Advertising Id'
  }
};

/**
 * A map of id logical types to the default field classification for Ids
 * @type {Object}
 */
const idFieldDataTypeClassification = idLogicalTypes.reduce((classification, idLogicalType) => {
  return Object.assign(classification, { [idLogicalType]: 'limitedDistribution' });
}, {});

/**
 * Creates a mapping of nonIdFieldLogicalTypes to default classification for that field
 * @type {Object}
 */
const nonIdFieldDataTypeClassification = Object.keys(nonIdFieldLogicalTypes).reduce(
  (classification, logicalType) =>
    Object.assign(classification, {
      [logicalType]: nonIdFieldLogicalTypes[logicalType].classification
    }),
  {}
);
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
    displayAs: 'Not an ID'
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

export {
  defaultFieldDataTypeClassification,
  classifiers,
  fieldIdentifierTypes,
  idLogicalTypes,
  nonIdFieldLogicalTypes
};
