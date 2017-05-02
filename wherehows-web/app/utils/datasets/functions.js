import { classifiers } from 'wherehows-web/constants';

/**
 * Builds a privacyCompliancePolicy map with default / unset
 *   values for non null properties
 */
const createPrivacyCompliancePolicy = () => {
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
 *   for non null properties as per Avro schema
 * @param {number} id
 */
const createSecuritySpecification = id => {
  const classification = classifiers.reduce((classification, classifier) => {
    classification[classifier] = [];
    return classification;
  }, {});

  const securitySpecification = {
    classification,
    datasetId: id,
    geographicAffinity: { affinity: '' },
    recordOwnerType: '',
    retentionPolicy: { retentionType: '' },
    datasetClassification: {}
  };

  return JSON.parse(JSON.stringify(securitySpecification));
};

export { createSecuritySpecification, createPrivacyCompliancePolicy };
