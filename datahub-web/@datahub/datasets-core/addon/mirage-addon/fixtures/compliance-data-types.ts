import { IComplianceDataType } from '@datahub/metadata-types/types/entity/dataset/compliance-data-types';

export default [
  {
    pii: true,
    idType: true,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'Member ID',
    $URN: 'urn:li:complianceDataType:MEMBER_ID',
    supportedFieldFormats: ['NUMERIC', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'],
    id: 'MEMBER_ID'
  },
  {
    pii: false,
    idType: true,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'Organization ID',
    $URN: 'urn:li:complianceDataType:COMPANY_ID',
    supportedFieldFormats: ['NUMERIC', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'],
    id: 'COMPANY_ID'
  },
  {
    pii: false,
    idType: true,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'Group ID',
    $URN: 'urn:li:complianceDataType:GROUP_ID',
    supportedFieldFormats: ['NUMERIC', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'],
    id: 'GROUP_ID'
  },
  {
    pii: false,
    idType: true,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'Mixed ID',
    $URN: 'urn:li:complianceDataType:MIXED_ID',
    supportedFieldFormats: ['URN'],
    id: 'MIXED_ID'
  },
  {
    pii: true,
    idType: true,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'Enterprise Profile ID',
    $URN: 'urn:li:complianceDataType:ENTERPRISE_PROFILE_ID',
    supportedFieldFormats: ['NUMERIC', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'],
    id: 'ENTERPRISE_PROFILE_ID'
  },
  {
    pii: false,
    idType: true,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'Enterprise Account ID',
    $URN: 'urn:li:complianceDataType:ENTERPRISE_ACCOUNT_ID',
    supportedFieldFormats: ['NUMERIC', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'],
    id: 'ENTERPRISE_ACCOUNT_ID'
  },
  {
    pii: false,
    idType: true,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'Contract ID',
    $URN: 'urn:li:complianceDataType:CONTRACT_ID',
    supportedFieldFormats: ['NUMERIC', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'],
    id: 'CONTRACT_ID'
  },
  {
    pii: true,
    idType: true,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'Seat ID',
    $URN: 'urn:li:complianceDataType:SEAT_ID',
    supportedFieldFormats: ['NUMERIC', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'],
    id: 'SEAT_ID'
  },
  {
    pii: false,
    idType: true,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'Advertiser ID',
    $URN: 'urn:li:complianceDataType:ADVERTISER_ID',
    supportedFieldFormats: ['NUMERIC', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'],
    id: 'ADVERTISER_ID'
  },
  {
    pii: true,
    idType: true,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'SlideShare User ID',
    $URN: 'urn:li:complianceDataType:SLIDESHARE_USER_ID',
    supportedFieldFormats: ['NUMERIC', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'],
    id: 'SLIDESHARE_USER_ID'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'CONFIDENTIAL',
    title: 'Name',
    $URN: 'urn:li:complianceDataType:NAME',
    supportedFieldFormats: [],
    id: 'NAME'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'CONFIDENTIAL',
    title: 'Email',
    $URN: 'urn:li:complianceDataType:EMAIL',
    supportedFieldFormats: [],
    id: 'EMAIL'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'CONFIDENTIAL',
    title: 'Phone',
    $URN: 'urn:li:complianceDataType:PHONE',
    supportedFieldFormats: [],
    id: 'PHONE'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'CONFIDENTIAL',
    title: 'Address',
    $URN: 'urn:li:complianceDataType:ADDRESS',
    supportedFieldFormats: [],
    id: 'ADDRESS'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'CONFIDENTIAL',
    title: 'Latitude & Longitude',
    $URN: 'urn:li:complianceDataType:LATITUDE_LONGITUDE',
    supportedFieldFormats: [],
    id: 'LATITUDE_LONGITUDE'
  },
  {
    pii: false,
    idType: false,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'City, State, Region, Country',
    $URN: 'urn:li:complianceDataType:CITY_STATE_REGION',
    supportedFieldFormats: [],
    id: 'CITY_STATE_REGION'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'CONFIDENTIAL',
    title: 'IP Address',
    $URN: 'urn:li:complianceDataType:IP_ADDRESS',
    supportedFieldFormats: [],
    id: 'IP_ADDRESS'
  },
  {
    pii: false,
    idType: false,
    defaultSecurityClassification: 'CONFIDENTIAL',
    title: 'Financial Number',
    $URN: 'urn:li:complianceDataType:FINANCIAL_NUMBER',
    supportedFieldFormats: [],
    id: 'FINANCIAL_NUMBER'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'HIGHLY_CONFIDENTIAL',
    title: 'Payment Info',
    $URN: 'urn:li:complianceDataType:PAYMENT_INFO',
    supportedFieldFormats: [],
    id: 'PAYMENT_INFO'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'HIGHLY_CONFIDENTIAL',
    title: 'Password & Credentials',
    $URN: 'urn:li:complianceDataType:PASSWORD_CREDENTIAL',
    supportedFieldFormats: [],
    id: 'PASSWORD_CREDENTIAL'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'HIGHLY_CONFIDENTIAL',
    title: 'Authentication Token',
    $URN: 'urn:li:complianceDataType:AUTHENTICATION_TOKEN',
    supportedFieldFormats: [],
    id: 'AUTHENTICATION_TOKEN'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'HIGHLY_CONFIDENTIAL',
    title: 'Message',
    $URN: 'urn:li:complianceDataType:MESSAGE',
    supportedFieldFormats: [],
    id: 'MESSAGE'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'HIGHLY_CONFIDENTIAL',
    title: 'National ID',
    $URN: 'urn:li:complianceDataType:NATIONAL_ID',
    supportedFieldFormats: [],
    id: 'NATIONAL_ID'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'CONFIDENTIAL',
    title: 'Social Network ID',
    $URN: 'urn:li:complianceDataType:SOCIAL_NETWORK_ID',
    supportedFieldFormats: [],
    id: 'SOCIAL_NETWORK_ID'
  },
  {
    pii: false,
    idType: false,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'Event Time',
    $URN: 'urn:li:complianceDataType:EVENT_TIME',
    supportedFieldFormats: [],
    id: 'EVENT_TIME'
  },
  {
    pii: false,
    idType: false,
    defaultSecurityClassification: 'LIMITED_DISTRIBUTION',
    title: 'Transaction Time',
    $URN: 'urn:li:complianceDataType:TRANSACTION_TIME',
    supportedFieldFormats: [],
    id: 'TRANSACTION_TIME'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'CONFIDENTIAL',
    title: 'Cookies, Beacons, Browser ID',
    $URN: 'urn:li:complianceDataType:COOKIE_BEACON_BROWSER_ID',
    supportedFieldFormats: [],
    id: 'COOKIE_BEACON_BROWSER_ID'
  },
  {
    pii: true,
    idType: false,
    defaultSecurityClassification: 'CONFIDENTIAL',
    title: 'Device ID, Advertising ID',
    $URN: 'urn:li:complianceDataType:DEVICE_ID_ADVERTISING_ID',
    supportedFieldFormats: [],
    id: 'DEVICE_ID_ADVERTISING_ID'
  }
].map(type => ({
  ...type,
  supportedFieldFormats: type.supportedFieldFormats.map(format => ({ id: format, description: '' }))
})) as Array<IComplianceDataType>;
