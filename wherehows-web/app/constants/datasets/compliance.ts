/**
 * String indicating that the user affirms or ignored a field suggestion
 */
enum SuggestionIntent {
  accept = 'accept',
  ignore = 'ignore'
}

/**
 * Defines the string values that are allowed for a classification
 */
enum Classification {
  Confidential = 'confidential',
  LimitedDistribution = 'limitedDistribution',
  HighlyConfidential = 'highlyConfidential'
}

/**
 * Defines the string values for an id logical type
 * @enum {string}
 */
enum IdLogicalType {
  Id = 'ID',
  Urn = 'URN',
  ReversedUrn = 'REVERSED_URN',
  CompositeUrn = 'COMPOSITE_URN'
}

/**
 * Defines the string values for a custom id logical tyoe
 * @enum {string}
 */
enum CustomIdLogicalType {
  Custom = 'CUSTOM_ID'
}

/**
 * Enum of values for non id / generic logical types
 * @enum {string}
 */
enum NonIdLogicalType {
  Name = 'NAME',
  Email = 'EMAIL',
  Phone = 'PHONE',
  Address = 'ADDRESS',
  LatitudeLongitude = 'LATITUDE_LONGITUDE',
  CityStateRegion = 'CITY_STATE_REGION',
  IpAddress = 'IP_ADDRESS',
  FinancialNumber = 'FINANCIAL_NUMBER',
  PaymentInfo = 'PAYMENT_INFO',
  PasswordCredential = 'PASSWORD_CREDENTIAL',
  AuthenticationToken = 'AUTHENTICATION_TOKEN',
  Message = 'MESSAGE',
  NationalId = 'NATIONAL_ID',
  SocialNetworkId = 'SOCIAL_NETWORK_ID',
  EventTime = 'EVENT_TIME',
  TransactionTime = 'TRANSACTION_TIME',
  CookieBeaconBrowserId = 'COOKIE_BEACON_BROWSER_ID',
  DeviceIdAdvertisingId = 'DEVICE_ID_ADVERTISING_ID'
}

/**
 * String values for field Identifier type
 * @enum {string}
 */
enum FieldIdValues {
  None = 'NONE',
  MemberId = 'MEMBER_ID',
  SubjectMemberId = 'SUBJECT_MEMBER_ID',
  GroupId = 'GROUP_ID',
  CompanyId = 'COMPANY_ID',
  MixedId = 'MIXED_ID',
  CustomId = 'CUSTOM_ID',
  EnterpriseProfileId = 'ENTERPRISE_PROFILE_ID',
  EnterpriseAccountId = 'ENTERPRISE_ACCOUNT_ID',
  ContractId = 'CONTRACT_ID',
  SeatId = 'SEAT_ID',
  AdvertiserId = 'ADVERTISER_ID',
  SlideshareUserId = 'SLIDESHARE_USER_ID'
}

/**
 * Describes the index signature for the nonIdFieldLogicalTypes object
 * TODO: on release of TS 2.16, convert to type in previous commit i.e. revert this commit
 * and restrict keys to value in enum
 * @interface INonIdLogicalTypesSignature
 */
interface INonIdLogicalTypesSignature {
  [prop: string]: {
    classification: Classification;
    displayAs: string;
  };
}

/**
 * Describes the properties on a field identifier object for ui rendering
 * @interface IFieldIdProps
 */
interface IFieldIdProps {
  value: FieldIdValues;
  isId: boolean;
  displayAs: string;
}

/**
 * Describes the index signature for fieldIdentifierTypes
 * @interface IFieldIdTypes
 */
interface IFieldIdTypes {
  [prop: string]: IFieldIdProps;
}

/**
 * A list of id logical types
 * @type {Array<IdLogicalType>}
 */
const idLogicalTypes = Object.values(IdLogicalType).sort() as Array<IdLogicalType>;

/**
 * A list of custom logical types that may be treated ids but have a different behaviour from regular ids
 * @type {Array<CustomIdLogicalType>}
 */
const customIdLogicalTypes = Object.values(CustomIdLogicalType) as Array<CustomIdLogicalType>;

/**
 * List of non Id field data type classifications
 * @type {Array<NonIdLogicalType>}
 */
const genericLogicalTypes = Object.values(NonIdLogicalType).sort() as Array<NonIdLogicalType>;

// Default mapping of field data types to security classification
// https://iwww.corp.linkedin.com/wiki/cf/display/DWH/List+of+Metadata+for+Data+Sets
const nonIdFieldLogicalTypes: INonIdLogicalTypesSignature = {
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
 * A map of identifier types for fields on a dataset
 * @type {IFieldIdTypes}
 */
const fieldIdentifierTypes: IFieldIdTypes = {
  none: {
    value: FieldIdValues.None,
    isId: false,
    displayAs: 'Not an ID'
  },
  member: {
    value: FieldIdValues.MemberId,
    isId: true,
    displayAs: 'Member ID'
  },
  subjectMember: {
    value: FieldIdValues.SubjectMemberId,
    isId: true,
    displayAs: 'Member ID (Subject Owner)'
  },
  group: {
    value: FieldIdValues.GroupId,
    isId: true,
    displayAs: 'Group ID'
  },
  organization: {
    value: FieldIdValues.CompanyId,
    isId: true,
    displayAs: 'Organization ID'
  },
  generic: {
    value: FieldIdValues.MixedId,
    isId: false,
    displayAs: 'Mixed'
  },
  custom: {
    value: FieldIdValues.CustomId,
    isId: false,
    // Although rendered as though an id, it's custom and from a UI perspective does not share a key similarity to other
    // ids, a logicalType / (field format) is not required to update this fields properties
    displayAs: 'Custom ID'
  },
  enterpriseProfile: {
    value: FieldIdValues.EnterpriseProfileId,
    isId: true,
    displayAs: 'Enterprise Profile ID'
  },
  enterpriseAccount: {
    value: FieldIdValues.EnterpriseAccountId,
    isId: true,
    displayAs: 'Enterprise Account ID'
  },
  contract: {
    value: FieldIdValues.ContractId,
    isId: true,
    displayAs: 'Contract ID'
  },
  seat: {
    value: FieldIdValues.SeatId,
    isId: true,
    displayAs: 'Seat ID'
  },
  advertiser: {
    value: FieldIdValues.AdvertiserId,
    isId: true,
    displayAs: 'Advertiser ID'
  },
  slideshare: {
    value: FieldIdValues.SlideshareUserId,
    isId: true,
    displayAs: 'SlideShare User ID'
  }
};

export {
  Classification,
  FieldIdValues,
  IFieldIdProps,
  IdLogicalType,
  idLogicalTypes,
  CustomIdLogicalType,
  customIdLogicalTypes,
  NonIdLogicalType,
  genericLogicalTypes,
  nonIdFieldLogicalTypes,
  fieldIdentifierTypes,
  SuggestionIntent
};
