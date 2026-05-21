/**
 * Shared constants for structured properties tests
 */

export const TEST_DATA = {
  // Dataset URN for all entity-level tests
  DATASET_URN: 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)',

  // Field name for schema field tests
  FIELD_NAME: 'shipment_info',

  // Property values used in tests
  PROPERTY_VALUE: 'Playwright structured prop value',
  PROPERTY_VALUE_UPDATED: 'Updated Playwright structured prop value',
};

export const TOAST_MESSAGES = {
  PROPERTY_CREATED: 'created',
  PROPERTY_ADDED: 'added',
  PROPERTY_UPDATED: 'updated',
  PROPERTY_DELETED: 'deleted',
  PROPERTY_REMOVED: 'removed',
};

export const TIMEOUTS = {
  DROPDOWN_VISIBLE: 10000,
  PROPERTY_IN_DROPDOWN: 45000,
  GENERAL: 15000,
  DRAWER_VISIBLE: 15000,
  FIELD_BUTTON_VISIBLE: 30000,
  SETTING_PROPAGATION: 6000,
  PAGE_STABILIZE: 500,
  UPDATE_PROPAGATION: 3000,
};
