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
  Confidential = 'CONFIDENTIAL',
  LimitedDistribution = 'LIMITED_DISTRIBUTION',
  HighlyConfidential = 'HIGHLY_CONFIDENTIAL',
  Internal = 'INTERNAL',
  Public = 'PUBLIC'
}

/**
 * Defines the string values for an id logical type
 * @enum {string}
 */
enum IdLogicalType {
  Numeric = 'NUMERIC',
  Urn = 'URN',
  ReversedUrn = 'REVERSED_URN',
  CompositeUrn = 'COMPOSITE_URN'
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
enum ComplianceFieldIdValue {
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

export { Classification, ComplianceFieldIdValue, IdLogicalType, NonIdLogicalType, SuggestionIntent };
