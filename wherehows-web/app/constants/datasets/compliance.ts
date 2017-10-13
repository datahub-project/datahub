/**
 * Defines the string values that are allowed for a classification
 */
enum Classification {
  Confidential = 'confidential',
  LimitedDistribution = 'limitedDistribution',
  HighlyConfidential = 'highlyConfidential'
}

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

type NonIdLogicalTypesSignature = {
  [K in NonIdLogicalType]: {
    classification: Classification;
    displayAs: string;
  }
};

// Default mapping of field data types to security classification
// https://iwww.corp.linkedin.com/wiki/cf/display/DWH/List+of+Metadata+for+Data+Sets
const nonIdFieldLogicalTypes: NonIdLogicalTypesSignature = {
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

export { Classification, NonIdLogicalType, nonIdFieldLogicalTypes };
