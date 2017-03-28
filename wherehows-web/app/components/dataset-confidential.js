import Ember from 'ember';

const {
  get,
  set,
  isBlank,
  computed,
  getWithDefault,
  Component
} = Ember;

// String constant identifying the classified fields on the security spec
const sourceClassificationKey = 'securitySpecification.classification';
const classifiers = [
  'confidential',
  'highlyConfidential',
  'limitedDistribution',
  'mustBeEncrypted',
  'mustBeMasked'
];
/**
 * Takes a string, returns a formatted string. Niche , single use case
 * for now, so no need to make into a helper
 * @param {String} string
 */
const formatAsCapitalizedStringWithSpaces = string =>
  string.replace(/[A-Z]/g, match => ` ${match}`).capitalize();

export default Component.extend({
  sortColumnWithName: 'identifierField',
  filterBy: 'identifierField',
  sortDirection: 'asc',
  searchTerm: '',

  // Map classifiers to options better consumed by  drop down
  classifiers: ['', ...classifiers].map(value => ({
    value,
    label: value ? formatAsCapitalizedStringWithSpaces(value) : 'Please Select'
  })),

  /**
   * Creates a lookup table of fieldNames to classification
   *   Also, the expectation is that the association from fieldName -> classification
   *   is one-to-one hence no check to ensure a fieldName gets clobbered
   *   in the lookup assignment
   */
  fieldNameToClass: computed(`${sourceClassificationKey}`, function () {
    const sourceClasses = getWithDefault(this, sourceClassificationKey, []);
    // Creates a lookup table of fieldNames to classification
    //   Also, the expectation is that the association from fieldName -> classification
    //   is one-to-one hence no check to ensure a fieldName gets clobbered
    //   in the lookup assignment
    return Object.keys(sourceClasses).reduce((lookup, classificationKey) =>
        // For the provided classificationKey, iterate over it's fieldNames,
        //   and assign the classificationKey to the fieldName in the table
        (sourceClasses[classificationKey] || []).reduce((lookup, fieldName) => {
          // cKey -> 1...fieldNameList => fieldName -> cKey
          lookup[fieldName] = classificationKey;
          return lookup;
        }, lookup),
      {}
    );
  }),

  /**
   * Lists all the dataset fields found in the `columns` api, and intersects
   *   each with the currently classified field names in
   *   securitySpecification.classification or null if not found
   */
  classificationDataFields: computed(
    `${sourceClassificationKey}.${classifiers}`,
    'fieldNameToClass',
    'schemaFieldNamesMappedToDataTypes',
    function () {
      // Set default or if already in policy, retrieve current values from
      //   privacyCompliancePolicy.compliancePurgeEntities
      return getWithDefault(
        this, 'schemaFieldNamesMappedToDataTypes', []
      ).map(({fieldName, dataType}) => ({
        dataType,
        identifierField: fieldName,
        classification: get(this, `fieldNameToClass.${fieldName}`) || null
      }));
    }
  ),

  actions: {
    /**
     * Toggles the provided identifierField onto a classification list
     *   on securitySpecification.classification, identified by the provided
     *   classKey.
     * @param {String} identifierField field on the dataset
     * @param {String} classKey the name of the class to add, or potentially
     *   remove the identifierField from
     */
    updateClassification({identifierField}, {value: classKey}) {
      const currentClass = get(this, `fieldNameToClass.${identifierField}`);
      // Since the association from identifierField -> classification is 1-to-1
      //  ensure that we do not currently have this identifierField
      // in any other classification lists by checking that the lookup is void
      if (!isBlank(currentClass)) {
        // Get the current classification list
        const currentClassification = get(
          this, `${sourceClassificationKey}.${currentClass}`
        );
        // Remove identifierField from list
        currentClassification.removeObject(identifierField);
      }

      if (classKey) {
        // Get the candidate list
        let classification = get(this, `${sourceClassificationKey}.${classKey}`);
        // In the case that the list is not pre-populated,
        //  the value will be the default null, array ops won't work here
        //  ...so make array
        if (!classification) {
          classification = set(this, `${sourceClassificationKey}.${classKey}`, []);
        }

        // Finally perform operation
        classification.addObject(identifierField);
      }
    },

    // Notify controller to propagate changes
    saveSecuritySpecification() {
      get(this, 'onSave')();
      return false;
    },

    // Rolls back changes made to the compliance spec to current
    // server state
    resetSecuritySpecification() {
      get(this, 'onReset')();
    }
  }
});
