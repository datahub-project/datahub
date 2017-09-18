import Ember from 'ember';

/**
 * Defines the string values that are allowed for a classification
 */
enum Classification {
  Confidential = 'confidential',
  LimitedDistribution = 'limitedDistribution',
  HighlyConfidential = 'highlyConfidential'
}

/**
 * Describes the index signature for the nonIdFieldLogicalTypes object
 */
interface INonIdLogicalTypes {
  [prop: string]: {
    classification: Classification;
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
    classification: Classification.Confidential,
    displayAs: 'Name'
  },
  EMAIL: {
    classification: Classification.Confidential,
    displayAs: 'E-mail'
  },
  PHONE: {
    classification: Classification.Confidential,
    displayAs: 'Phone Number'
  },
  ADDRESS: {
    classification: Classification.Confidential,
    displayAs: 'Address'
  },
  LATITUDE_LONGITUDE: {
    classification: Classification.Confidential,
    displayAs: 'Latitude and Longitude'
  },
  CITY_STATE_REGION: {
    classification: Classification.LimitedDistribution,
    displayAs: 'City, State, Region, etcetera'
  },
  IP_ADDRESS: {
    classification: Classification.Confidential,
    displayAs: 'IP Address'
  },
  FINANCIAL_NUMBER: {
    classification: Classification.Confidential,
    displayAs: 'Financial Number'
  },
  PAYMENT_INFO: {
    classification: Classification.HighlyConfidential,
    displayAs: 'Payment Info'
  },
  PASSWORD_CREDENTIAL: {
    classification: Classification.HighlyConfidential,
    displayAs: 'Password and Credentials'
  },
  AUTHENTICATION_TOKEN: {
    classification: Classification.HighlyConfidential,
    displayAs: 'Authentication Token'
  },
  MESSAGE: {
    classification: Classification.HighlyConfidential,
    displayAs: 'Message'
  },
  NATIONAL_ID: {
    classification: Classification.HighlyConfidential,
    displayAs: 'National Id'
  },
  SOCIAL_NETWORK_ID: {
    classification: Classification.Confidential,
    displayAs: 'Social Network Id'
  },
  EVENT_TIME: {
    classification: Classification.LimitedDistribution,
    displayAs: 'Event Time'
  },
  TRANSACTION_TIME: {
    classification: Classification.LimitedDistribution,
    displayAs: 'Transaction Time'
  },
  COOKIE_BEACON_BROWSER_ID: {
    classification: Classification.Confidential,
    displayAs: 'Cookies and Beacons and Browser Id'
  },
  DEVICE_ID_ADVERTISING_ID: {
    classification: Classification.Confidential,
    displayAs: 'Device Id and Advertising Id'
  }
};

/**
 * List of non Id field data type classifications
 * @type {Array}
 */
const genericLogicalTypes = Object.keys(nonIdFieldLogicalTypes).sort();

/**
 * A map of id logical types including custom ids to the default field classification for Ids
 * @type {Object}
 */
const idFieldDataTypeClassification: { [K: string]: Classification.LimitedDistribution } = [
  ...customIdLogicalTypes,
  ...idLogicalTypes
].reduce(
  (classification, idLogicalType) =>
    Object.assign(classification, { [idLogicalType]: Classification.LimitedDistribution }),
  {}
);

/**
 * Creates a mapping of nonIdFieldLogicalTypes to default classification for that field
 * @type {Object}
 */
const nonIdFieldDataTypeClassification: { [K: string]: Classification } = Object.keys(nonIdFieldLogicalTypes).reduce(
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
  contract: {
    value: 'CONTRACT_ID',
    isId: true,
    displayAs: 'Contract ID'
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
  }
};

/**
 * Checks if the identifierType is a mixed Id
 * @param {string} identifierType
 */
const isMixedId = (identifierType: string) => identifierType === fieldIdentifierTypes.generic.value;
/**
 * Checks if the identifierType is a custom Id
 * @param {string} identifierType
 */
const isCustomId = (identifierType: string) => identifierType === fieldIdentifierTypes.custom.value;

/**
 * Checks if the identifier format is only allowed numeric (ID) or URN values
 * @param {string} identifierType
 */
const isNumericOrUrnOnly = (identifierType: string) =>
  [
    fieldIdentifierTypes.enterpriseProfile.value,
    fieldIdentifierTypes.contract.value,
    fieldIdentifierTypes.seat.value,
    fieldIdentifierTypes.advertiser.value
  ].includes(identifierType);

/**
 * Checks is the identifierType is only allowed Id fields
 * @param identifierType
 */
const isIdOnly = (identifierType: string) => identifierType === fieldIdentifierTypes.enterpriseAccount.value;

/**
 * Checks if an identifierType has a predefined/immutable value for the field format, i.e. should not be changed by
 * the end user
 * @param {string} identifierType the identifierType to check against
 * @return {boolean}
 */
const hasPredefinedFieldFormat = (identifierType: string) => {
  return isMixedId(identifierType) || isCustomId(identifierType) || isIdOnly(identifierType);
};

/**
 * Gets the default logical type for an identifier type
 * @param {string} identifierType
 * @return {string | void}
 */
const getDefaultLogicalType = (identifierType: string): string | void => {
  if (isMixedId(identifierType)) {
    return 'URN';
  }

  if (isIdOnly(identifierType)) {
    return 'ID';
  }
};

/**
 * Returns a list of logicalType mappings for displaying its value and a label by logicalType
 * @param {String} logicalType
 */
const logicalTypeValueLabel = (logicalType: string) =>
  (<{ [K: string]: Array<string> }>{
    id: idLogicalTypes,
    generic: genericLogicalTypes
  })[logicalType].map(value => ({
    value,
    label: nonIdFieldLogicalTypes[value]
      ? nonIdFieldLogicalTypes[value].displayAs
      : value.replace(/_/g, ' ').replace(/([A-Z]{3,})/g, value => Ember.String.capitalize(value.toLowerCase()))
  }));

// Map logicalTypes to options consumable by DOM
const logicalTypesForIds = logicalTypeValueLabel('id');

// Map generic logical type to options consumable in DOM
const logicalTypesForGeneric = logicalTypeValueLabel('generic');

export {
  defaultFieldDataTypeClassification,
  classifiers,
  fieldIdentifierTypes,
  idLogicalTypes,
  customIdLogicalTypes,
  nonIdFieldLogicalTypes,
  isMixedId,
  isCustomId,
  isNumericOrUrnOnly,
  isIdOnly,
  hasPredefinedFieldFormat,
  logicalTypesForIds,
  logicalTypesForGeneric,
  getDefaultLogicalType
};
