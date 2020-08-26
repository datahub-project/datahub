/**
 * The business / semantic meaning or data type of data fields. http://go/gdpr-taxonomy
 * @export
 * @namespace Dataset
 * @enum {string}
 */
export enum ComplianceDataType {
  // "Personal physical address"
  Address = 'ADDRESS',
  // "ID for an LMS advertiser"
  AdvertiserId = 'ADVERTISER_ID',
  // "ID for a shared URL (deprecated version of INGESTED_CONTENT_ID)"
  ArticleId = 'ARTICLE_ID',
  // "Authentication token, including third party tokens"
  AuthenticationToken = 'AUTHENTICATION_TOKEN',
  // "City, State, Region, etc"
  CityStateRegion = 'CITY_STATE_REGION',
  // "ID for companies or organizations that created content at LinkedIn"
  CompanyId = 'COMPANY_ID',
  // "ID for a content topic. See go/contenttopic for more details"
  ContentTopicId = 'CONTENT_TOPIC_ID',
  // "id for a contract, a grouping of enterprise users for an lts product"
  ContractId = 'CONTRACT_ID',
  // "Cookies, beacons, browser ID"
  CookieBeaconBrowserId = 'COOKIE_BEACON_BROWSER_ID',
  // "[Deprecated]: Use CUSTOM FieldFormat instead"
  CustomId = 'CUSTOM_ID',
  // "Date of birth of a person"
  DateOfBirth = 'DATE_OF_BIRTH',
  // "Device ID, Advertising ID"
  DeviceIdAdvertisingId = 'DEVICE_ID_ADVERTISING_ID',
  // "ID for a contract within Elevate (LEAP). Corresponds to a LeapContractUrn."
  ElevateContractId = 'ELEVATE_CONTRACT_ID',
  // "ID for a user on a contract within Elevate (LEAP). Corresponds to a LeapSeatV2Urn"
  ElevateSeatId = 'ELEVATE_SEAT_ID',
  // "Personal email address"
  Email = 'EMAIL',
  // "ID for enterprise account"
  EnterpriseAccountId = 'ENTERPRISE_ACCOUNT_ID',
  // "ID for enterprise profile"
  EnterpriseProfileId = 'ENTERPRISE_PROFILE_ID',
  // "Time of an event, e.g. header.time"
  EventTime = 'EVENT_TIME',
  // "Financial number: order amount, payment amount, etc"
  FinancialNumber = 'FINANCIAL_NUMBER',
  // "To capture free-formed user generated content. See go/Metadata/UGC"
  FreeformedUgc = 'FREEFORMED_UGC',
  // "ID for LinkedIn groups"
  GroupId = 'GROUP_ID',
  // "ID that is standard protocol-based, verifiable, globally unique, and allow communication with members. See https://iwww.corp.linkedin.com/wiki/cf/pages/viewpage.action?pageId=102778068 for more details"
  Handles = 'HANDLES',
  // "ID for a HireStream in AutomatedSourcing. Note: this is a legacy ID which is now derived from a SourcingChannelUrn. It is used to ensure that legacy data is compliant."
  HireStreamId = 'HIRE_STREAM_ID',
  // "ID for a shared URL. See go/contentingestion for more details"
  IngestedContentId = 'INGESTED_CONTENT_ID',
  // "ID for an interest. See go/interesttagging for more details"
  InterestId = 'INTEREST_ID',
  // "IPv4 or IPv6 address",
  IpAddress = 'IP_ADDRESS',
  // "ID for a job posting"
  JobId = 'JOB_ID',
  // "Latitude and Longitude"
  LatitudeLongitude = 'LATITUDE_LONGITUDE',
  // "Logs that can potentially contain PII"
  LogsPii = 'LOGS_PII',
  // "Lynda User ID of the Lynda Enterprise Account Master Admin"
  LyndaMasterAdminId = 'LYNDA_MASTER_ADMIN_ID',
  // "User ID of Lynda.com user"
  LyndaUserId = 'LYNDA_USER_ID',
  // "ID for LinkedIn members"
  MemberId = 'MEMBER_ID',
  // "Member's photo"
  MemberPhoto = 'MEMBER_PHOTO',
  // "Private message content"
  Message = 'MESSAGE',
  // "[Deprecated] Specify all IDs explicitly"
  MixedId = 'MIXED_ID',
  // "Name: first name, last name, full name"
  Name = 'NAME',
  // "National ID number, SSN, driver license"
  NationalId = 'NATIONAL_ID',
  // "None of the other types apply"
  None = 'NONE',
  // "[Deprecated] Use LOGS_PII for logs containing PII and UNSTRUCTURED_PII for other unstructured data"
  OtherPii = 'OTHER_PII',
  // "Password and credentials"
  PasswordCredential = 'PASSWORD_CREDENTIAL',
  // "Payment info: credit card, bank account"
  PaymentInfo = 'PAYMENT_INFO',
  // "Phone numbers, phone number URN"
  Phone = 'PHONE',
  // "Member Profile url"
  ProfileUrl = 'PROFILE_URL',
  // "Salary data"
  Salary = 'SALARY',
  // "ID for a user of an LTS enterprise product"
  SeatId = 'SEAT_ID',
  // "ID for a slideshare user"
  SlideshareUserId = 'SLIDESHARE_USER_ID',
  // "Social network ID: facebook ID, WeChat ID"
  SocialNetworkId = 'SOCIAL_NETWORK_ID',
  // "Transaction time, e.g. CREATED_DATE, RECONCILED_DATE, ORDER_DATE"
  TransactionTime = 'TRANSACTION_TIME',
  // "ID for an ugc post. UGC stands for User Generated Content aka sharing on LinkedIn, publishing articles, image/video shares. See go/ugcbackend for more details"
  UgcId = 'UGC_ID',
  // "Unstructured data, e.g. serialized blob, that can contain PII"
  UnstructuredPii = 'UNSTRUCTURED_PII',
  // "Account ID in Zuora for Lynda.com users
  ZuoraAccountId = 'ZUORA_ACCOUNT_ID'
}
