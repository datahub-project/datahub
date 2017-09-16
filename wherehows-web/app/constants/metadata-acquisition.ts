type ClassificationUnion = 'confidential' | 'limitedDistribution' | 'highlyConfidential';
interface INonIdLogicalTypes {
  [prop: string]: {
    classification: ClassificationUnion;
    displayAs: string;
  };
}
/**
 * A list of id logical types
 * @type {Array.<String>}
 */
const idLogicalTypes = ['ID', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'];

/**
 * A list of custom logical types that may be treated ids but have a different behaviour from regular ids
 * @type {Array.<String>}
 */
const customIdLogicalTypes = ['CUSTOM_ID'];

// Default mapping of field data types to security classification
// https://iwww.corp.linkedin.com/wiki/cf/display/DWH/List+of+Metadata+for+Data+Sets
const nonIdFieldLogicalTypes: INonIdLogicalTypes = {
  NAME: {
    classification: 'confidential',
    displayAs: 'Name'
  },
  EMAIL: {
    classification: 'confidential',
    displayAs: 'E-mail'
  },
  PHONE: {
    classification: 'confidential',
    displayAs: 'Phone Number'
  },
  ADDRESS: {
    classification: 'confidential',
    displayAs: 'Address'
  },
  LATITUDE_LONGITUDE: {
    classification: 'confidential',
    displayAs: 'Latitude and Longitude'
  },
  CITY_STATE_REGION: {
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
  PASSWORD_CREDENTIAL: {
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
  COOKIE_BEACON_BROWSER_ID: {
    classification: 'confidential',
    displayAs: 'Cookies and Beacons and Browser Id'
  },
  DEVICE_ID_ADVERTISING_ID: {
    classification: 'confidential',
    displayAs: 'Device Id and Advertising Id'
  }
};

/**
 * A map of id logical types including custom ids to the default field classification for Ids
 * @type {Object}
 */
const idFieldDataTypeClassification = [
  ...customIdLogicalTypes,
  ...idLogicalTypes
].reduce((classification, idLogicalType) => {
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
    value: 'NONE',
    isId: false,
    displayAs: 'Not an ID'
  },
  member: {
    value: 'MEMBER_ID',
    isId: true,
    displayAs: 'Member ID'
  },
  subjectMember: {
    value: 'SUBJECT_MEMBER_ID',
    isId: true,
    displayAs: 'Member ID (Subject Owner)'
  },
  group: {
    value: 'GROUP_ID',
    isId: true,
    displayAs: 'Group ID'
  },
  organization: {
    value: 'COMPANY_ID',
    isId: true,
    displayAs: 'Organization ID'
  },
  generic: {
    value: 'MIXED_ID',
    isId: false,
    displayAs: 'Mixed'
  },
  custom: {
    value: 'CUSTOM_ID',
    isId: false,
    // Although rendered as though an id, it's custom and from a UI perspective does not share a key similarity to other
    // ids, a logicalType / (field format) is not required to update this fields properties
    displayAs: 'Custom ID'
  },
  enterpriseProfile: {
    value: 'ENTERPRISE_PROFILE_ID',
    isId: true,
    displayAs: 'Enterprise Profile ID'
  },
  enterpriseAccount: {
    value: 'ENTERPRISE_ACCOUNT_ID',
    isId: true,
    displayAs: 'Enterprise Account ID'
  },
  seat: {
    value: 'SEAT_ID',
    isId: true,
    displayAs: 'Seat ID'
  },
  advertiser: {
    value: 'ADVERTISER_ID',
    isId: true,
    displayAs: 'Advertiser ID'
  },
  contract: {
    value: 'CONTRACT_ID',
    isId: true,
    displayAs: 'Contract ID'
  }
};

export {
  defaultFieldDataTypeClassification,
  classifiers,
  fieldIdentifierTypes,
  idLogicalTypes,
  customIdLogicalTypes,
  nonIdFieldLogicalTypes
};
