/**
 * Builds a privacyCompliancePolicy map with default / unset
 *   values for non null properties
 */
export const createPrivacyCompliancePolicy = () => {
  const policy = {
    // default to first item in compliance types list
    complianceType: 'AUTO_PURGE',
    compliancePurgeEntities: []
  };

  // Ensure we deep clone map to prevent mutation from consumers
  return JSON.parse(JSON.stringify(policy));
};

/**
 * Builds a securitySpecification map with default / unset values
 *   for non null properties as per avro schema
 * @param {number} id
 */
export const createSecuritySpecification = id => {
  const classification = ['highlyConfidential', 'confidential'].reduce(
    (classification, classifier) => {
      classification[classifier] = [];
      return classification;
    },
    {}
  );

  const securitySpecification = {
    classification,
    datasetId: id,
    geographicAffinity: { affinity: '' },
    recordOwnerType: '',
    retentionPolicy: { retentionType: '' }
  };

  return JSON.parse(JSON.stringify(securitySpecification));
};
