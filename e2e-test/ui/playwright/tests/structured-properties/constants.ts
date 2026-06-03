/**
 * Test data constants for structured properties tests
 *
 * Uses separate fields/datasets per test suite to enable true parallel execution.
 * When tests run in parallel, they don't interfere with each other's data.
 *
 * Dataset URNs are seeded via fixtures/data.json (MCE format for CI ingestion).
 */

export const TEST_DATA = {
  // Separate datasets for entity-level tests (parallel-safe isolation)
  ENTITY_LEVEL_DATASET_ADD: 'urn:li:dataset:(urn:li:dataPlatform:hdfs,PlaywrightStructuredPropAddDataset,PROD)',
  ENTITY_LEVEL_DATASET_REMOVE: 'urn:li:dataset:(urn:li:dataPlatform:hdfs,PlaywrightStructuredPropRemoveDataset,PROD)',
  ENTITY_LEVEL_DATASET_EDIT: 'urn:li:dataset:(urn:li:dataPlatform:hdfs,PlaywrightStructuredPropEditDataset,PROD)',
  ENTITY_LEVEL_DATASET_HIDDEN: 'urn:li:dataset:(urn:li:dataPlatform:hdfs,PlaywrightStructuredPropHiddenDataset,PROD)',

  // Separate datasets for schema-field tests (parallel-safe isolation)
  SCHEMA_FIELD_ADD_DATASET:
    'urn:li:dataset:(urn:li:dataPlatform:hdfs,PlaywrightStructuredPropSchemaFieldAddDataset,PROD)',
  SCHEMA_FIELD_UPDATE_DATASET:
    'urn:li:dataset:(urn:li:dataPlatform:hdfs,PlaywrightStructuredPropSchemaFieldUpdateDataset,PROD)',
  SCHEMA_FIELD_REMOVE_DATASET:
    'urn:li:dataset:(urn:li:dataPlatform:hdfs,PlaywrightStructuredPropSchemaFieldRemoveDataset,PROD)',

  // Separate field names for schema-field tests (parallel-safe)
  SCHEMA_FIELD_ADD: 'schemaFieldAddField',
  SCHEMA_FIELD_UPDATE: 'schemaFieldUpdateField',
  SCHEMA_FIELD_REMOVE: 'schemaFieldRemoveField',

  // Property values used in tests
  PROPERTY_VALUE: 'testValue',
  PROPERTY_VALUE_UPDATED: 'updatedTestValue',
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
