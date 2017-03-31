import Ember from 'ember';

const {
  get,
  set,
  isBlank,
  computed,
  getWithDefault,
  setProperties,
  Component
} = Ember;

// String constant identifying the classified fields on the security spec
const sourceClassificationKey = 'securitySpecification.classification';
// TODO: DSS-6671 Extract to constants module
const classifiers = [
  'public',
  'confidential',
  'highlyConfidential'
];

const logicalTypes = [
  'EMAIL_ADDRESS',
  'PHONE_NUMBER',
  'IP_ADDRESS',
  'ADDRESS',
  'GEO_LOCATION',
  'FINANCIAL_NUMBER'
];

// TODO: DSS-6671 Extract to constants module
const successUpdating = 'Your changes have been successfully saved!';
const failedUpdating = 'Oops! We are having trouble updating this dataset at the moment.';
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
  classifiers: classifiers.map(value => ({
    value,
    label: formatAsCapitalizedStringWithSpaces(value)
  })),
  // Map logicalTypes to options better consumed by  drop down
  logicalTypes: ['', ...logicalTypes].map(value => {
    const label = value ?
      value.replace('_', ' ')
        .replace(/([A-Z]{3,})/g, f => f.toLowerCase().capitalize()) :
      'Please Select';

    return {
      value,
      label
    };
  }),

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
    return Object.keys(sourceClasses)
      .reduce((lookup, classificationKey) =>
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

  /**
   * TODO: DSS-6672 Extract to notifications service
   * Helper method to update user when an async server update to the
   * security specification is handled.
   * @param {XMLHttpRequest|Promise|jqXHR|*} request the server request
   */
  whenRequestCompletes(request) {
    Promise.resolve(request)
      .then(({return_code}) => {
        // The server api currently responds with an object containing
        //   a return_code when complete
        return return_code === 200 ?
          setProperties(this, {
            message: successUpdating,
            alertType: 'success'
          }) :
          Promise.reject(`Reason code for this is ${return_code}`);
      })
      .catch((err = '') => {
        setProperties(this, {
          message: `${failedUpdating} \n ${err}`,
          alertType: 'danger'
        });
      });
  },

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
          this,
          `${sourceClassificationKey}.${currentClass}`
        );
        // Remove identifierField from list
        currentClassification.removeObject(identifierField);
      }

      if (classKey) {
        // Get the candidate list
        let classification = get(
          this,
          `${sourceClassificationKey}.${classKey}`
        );
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

    /**
     * Notify controller to propagate changes
     * @return {Boolean}
     */
    saveSecuritySpecification() {
      this.whenRequestCompletes(get(this, 'onSave')());

      return false;
    },

    /**
     * Rolls back changes made to the compliance spec to current
     * server state
     */
    resetSecuritySpecification() {
      this.whenRequestCompletes(get(this, 'onReset')());
    }
  }
});
