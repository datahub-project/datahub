import Ember from 'ember';
import { datasetClassifiers } from 'wherehows-web/constants/dataset-classification';

const { assert, Logger: { warn } } = Ember;

/**
 * Builds a default shape for securitySpecification & privacyCompliancePolicy with default / unset values
 *   for non null properties as per Avro schema
 * @param {Number} datasetId id for the dataset that this privacy object applies to
 */
const createInitialComplianceInfo = datasetId => ({
  datasetId,
  // default to first item in compliance types list
  complianceType: 'AUTO_PURGE',
  complianceEntities: [],
  fieldClassification: {},
  datasetClassification: {},
  geographicAffinity: { affinity: '' },
  recordOwnerType: '',
  retentionPolicy: { retentionType: '' }
});

/**
 *
 * @type {{complianceEntities: {type: string, of: {type: string, keys: [*]}}, datasetClassification: {type: string, keys: (*)}, fieldClassification: {type: string}}}
 */
const policyShape = {
  complianceEntities: {
    type: 'array',
    of: {
      type: 'object',
      keys: [
        'identifierField:string',
        'identifierType:string',
        'securityClassification:string|object',
        'logicalType:string|object|undefined'
      ]
    }
  },
  datasetClassification: { type: 'object', keys: Object.keys(datasetClassifiers).map(key => `${key}:boolean`) }
};

/**
 * Checks that a policy is valid
 * @param candidatePolicy
 * @return {boolean}
 */
const isPolicyExpectedShape = (candidatePolicy = {}) => {
  const candidateMatchesShape = policyKey => {
    assert(
      `Expected each compliance policy attribute to be one of ${Object.keys(policyShape)}, but got ${policyKey}`,
      policyShape.hasOwnProperty(policyKey)
    );

    const policyProps = policyShape[policyKey];
    const expectedType = policyProps.type;
    const policyKeyValue = candidatePolicy[policyKey];
    const isValueExpectedType =
      expectedType === 'array' ? Array.isArray(policyKeyValue) : typeof policyKeyValue === expectedType;
    const typeDeclarations =
      {
        get array() {
          return policyProps.of.keys;
        },
        get object() {
          return policyProps.keys;
        }
      }[expectedType] || [];

    if (!policyKeyValue || !isValueExpectedType) {
      return false;
    }

    if (expectedType === 'array') {
      return policyKeyValue.every(value => {
        if (!value && typeof value !== policyProps.of.type) {
          warn(`Typedefs for ${policyKey} with value ${policyKeyValue} does not equal ${policyProps.of.type}`);
          return false;
        }

        return typeDeclarations.every(typeString => {
          const [key, type] = typeString.split(':');
          const result = type.includes(typeof value[key]);
          if (!result) {
            warn(`Typedefs for ${policyKey} don't include '${typeof value[key]}' for ${key}`);
          }

          return result;
        });
      });
    }

    if (expectedType === typeof {}) {
      return typeDeclarations.every(typeString => {
        const [key, type] = typeString.split(':');
        const result = type.includes(typeof policyKeyValue[key]);
        if (!result) {
          warn(`Typedefs for ${policyKey} don't include ${typeof policyKeyValue[key]} for ${key}`);
        }

        return result;
      });
    }
  };

  if (typeof candidatePolicy === 'object' && candidatePolicy) {
    return Object.keys(policyShape).every(candidateMatchesShape);
  }

  return false;
};

export { createInitialComplianceInfo, isPolicyExpectedShape };
