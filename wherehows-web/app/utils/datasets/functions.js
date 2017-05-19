/**
 * Builds a default shape for securitySpecification & privacyCompliancePolicy with default / unset values
 *   for non null properties as per Avro schema
 * @param {number} datasetId id for the dataset that this privacy object applies to
 */
const createInitialComplianceInfo = datasetId => ({
  datasetId,
  // default to first item in compliance types list
  complianceType: 'AUTO_PURGE',
  compliancePurgeEntities: [],
  fieldClassification: {},
  datasetClassification: {},
  geographicAffinity: { affinity: '' },
  recordOwnerType: '',
  retentionPolicy: { retentionType: '' }
});

export { createInitialComplianceInfo };
