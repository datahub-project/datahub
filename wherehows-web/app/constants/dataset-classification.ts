import { IFieldIdentifierOption } from 'wherehows-web/constants';

/**
 * List of string values for every dataset classifier
 * @enum {string}
 */
enum DatasetClassifiers {
  CONNECTIONS_FOLLOWERS_FOLLOWING = 'Connections + Followers + Following',
  PROFILE_DATA = 'Profile Data',
  MESSAGING_DATA = 'Messaging Data (Metadata + Content)',
  THIRD_PARTY_INTEGRATIONS = 'Third Party Integrations In Use',
  ACTIVITY = 'Activity (Newsfeed Posts + Shares + Likes)',
  SETTINGS = 'Settings (Mail Settings, All psettings values)',
  JOB_APPLICATION_FLOW_DATA = 'Job Application Flow Data (Job Application + AWLI + Resumes + Application Answers)',
  ENTERPRISE_PRODUCT_DATA = 'Enterprise Product Data',
  ACCOUNT_STATUS = 'Account Status',
  ADDRESS_BOOK_IMPORT_DATA = 'Address Book Import Data',
  MICROSOFT_DATA = 'Data from Microsoft',
  SUBSIDIARY_DATA = 'Data from companies LinkedIn acquired (Lynda, Slideshare, Connectifier, Bizo, etcetera)',
  THIRD_PARTY_DATA = 'Data from other Third-party Integrations',
  DEVICE_DATA = 'Device Data',
  SEARCH_HISTORY = 'Search History',
  COURSE_VIEWING_HISTORY = 'Course Viewing History',
  WVMP = "Who's Viewed My Profile",
  PROFILE_VIEWS_BY_ME = 'Profile Views (by me)',
  ADVERTISING_DATA = 'Advertising related (LMS) Data',
  USAGE_ERROR_CONNECTIVITY_DATA = 'Usage, Error Reporting, Connectivity Data',
  OTHER_CLICKSTREAM_BROWSING_DATA = 'Other Clickstream Data + Browsing history'
}

/**
 * Defines a type alias for a DatasetClassification interface, constrains the keys as attributes on DatasetClassifiers
 * and specifies value types
 */
type DatasetClassification = { [K in keyof typeof DatasetClassifiers]: boolean | void };

/**
 * Describes the interface for a DatasetClassification option
 * @interface IDatasetClassificationOption
 * @extends {(IFieldIdentifierOption<boolean | void>)}
 */
interface IDatasetClassificationOption extends IFieldIdentifierOption<boolean | void> {
  classifier: keyof typeof DatasetClassifiers;
}

export { DatasetClassifiers, DatasetClassification, IDatasetClassificationOption };
