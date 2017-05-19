/**
 * Builds a default shape for securitySpecification & privacyCompliancePolicy with default / unset values
 *   for non null properties as per Avro schema
 * @param {Number} datasetId id for the dataset that this privacy object applies to
 * @param {String} datasetUrn
 */
const createInitialComplianceInfo = (datasetId, datasetUrn) => ({
  datasetId,
  datasetUrn,
  // default to first item in compliance types list
  complianceType: 'AUTO_PURGE',
  complianceEntities: [],
  fieldClassification: {},
  datasetClassification: {},
  geographicAffinity: { affinity: '' },
  recordOwnerType: '',
  retentionPolicy: { retentionType: '' }
});

export { createInitialComplianceInfo };
