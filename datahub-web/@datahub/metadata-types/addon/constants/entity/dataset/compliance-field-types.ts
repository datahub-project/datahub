/**
 * Defines the string values that are allowed for a classification
 * @export
 * @namespace Dataset
 * @enum {string}
 */
export enum Classification {
  Confidential = 'CONFIDENTIAL',
  LimitedDistribution = 'LIMITED_DISTRIBUTION',
  HighlyConfidential = 'HIGHLY_CONFIDENTIAL',
  Internal = 'GENERAL',
  Public = 'PUBLIC'
}

/**
 * Defines the string values for a logical type that is a member ID
 * @export
 * @namespace Dataset
 * @enum {string}
 */
export enum MemberIdLogicalType {
  // Numerical format, 12345
  Numeric = 'NUMERIC',
  // URN format, urn:li:member:12345
  Urn = 'URN',
  // Reversed URN format, 12345:member:li:urn
  ReversedUrn = 'REVERSED_URN',
  // [Deprecated] Use CUSTOM format + pattern instead
  CompositeUrn = 'COMPOSITE_URN',
  // Any other non-standard format. A pattern for the value is expected to be provided
  Custom = 'CUSTOM',
  // Data is stored in reversible encoded/serialized/encrypted format
  Encoded = 'ENCODED',
  // Data is stored in irreversible hashed format
  Hashed = 'HASHED'
}

/**
 * Enum of values for non-member id / generic logical types
 * @export
 * @namespace Dataset
 * @enum {string}
 */
export enum NonMemberIdLogicalType {
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
 * Possible string values for field Identifier type in compliance
 * @export
 * @namespace Dataset
 * @enum {string}
 */
export enum ComplianceFieldIdValue {
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
 * String values for the keys in the export policy data types
 * @export
 * @namespace Dataset
 * @enum {string}
 */
export enum ExportPolicyKeys {
  UGC = 'containsUserGeneratedContent',
  UAGC = 'containsUserActionGeneratedContent',
  UDC = 'containsUserDerivedContent'
}
