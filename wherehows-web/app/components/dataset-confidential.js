import Ember from 'ember';
import isTrackingHeaderField from 'wherehows-web/utils/validators/tracking-headers';

const {
  get,
  set,
  isBlank,
  isPresent,
  computed,
  getWithDefault,
  setProperties,
  Component,
  String: { htmlSafe }
} = Ember;

// String constant identifying the classified fields on the security spec
const sourceClassificationKey = 'securitySpecification.classification';
// TODO: DSS-6671 Extract to constants module
const classifiers = [
  'confidential',
  'highlyConfidential'
];

const logicalTypes = [
  'EMAIL_ADDRESS',
  'PHONE_NUMBER',
  'IP_ADDRESS',
  'ADDRESS',
  'GEO_LOCATION',
  'CREDIT_CARD_OR_FINANCIAL_NUMBER',
  'ADDRESS_BOOK',
  'INMAILS_OR_MESSAGES',
  'PASSWORDS',
  'OTHER'
];

// TODO: DSS-6671 Extract to constants module
const successUpdating = 'Your changes have been successfully saved!';
const failedUpdating = 'Oops! We are having trouble updating this dataset at the moment.';
const missingTypes = 'Looks like some fields are marked as `Confidential` or ' +
  '`Highly Confidential` but do not have a specified `Field Format`?';
const hiddenTrackingFieldsMsg = htmlSafe(
  '<p>Hey! Just a heads up that some fields in this dataset have been hidden from the table(s) below. ' +
  'These are tracking fields for which we\'ve been able to predetermine the compliance classification.</p>' +
  '<p>For example: <code>header.memberId</code>, <code>requestHeader</code>. ' +
  'Hopefully, this saves you some scrolling!</p>'
);

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
  hiddenTrackingFields: hiddenTrackingFieldsMsg,

  // Map classifiers to options better consumed by  drop down
  classifiers: ['', ...classifiers].map(value => ({
    value,
    label: formatAsCapitalizedStringWithSpaces(value || 'notConfidential')
  })),
  // Map logicalTypes to options better consumed by  drop down
  logicalTypes: ['', ...logicalTypes].map(value => {
    const label = value ?
      value.replace(/_/g, ' ')
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
  fieldNameToClass: computed(
    `${sourceClassificationKey}.confidential.[]`,
    `${sourceClassificationKey}.highlyConfidential.[]`,
    function () {
      const sourceClasses = getWithDefault(this, sourceClassificationKey, []);
      // Creates a lookup table of fieldNames to classification
      //   Also, the expectation is that the association from fieldName -> classification
      //   is one-to-one hence no check to ensure a fieldName gets clobbered
      //   in the lookup assignment
      return Object.keys(sourceClasses)
        .reduce((lookup, classificationKey) =>
            // For the provided classificationKey, iterate over it's fieldNames,
            //   and assign the classificationKey to the fieldName in the table
            (sourceClasses[classificationKey] || []).reduce((lookup, field) => {
              const { identifierField } = field;
              // cKey -> 1...fieldNameList => fieldName -> cKey
              lookup[identifierField] = classificationKey;
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
    `${sourceClassificationKey}.confidential.[]`,
    `${sourceClassificationKey}.highlyConfidential.[]`,
    'fieldNameToClass',
    'schemaFieldNamesMappedToDataTypes',
    function () {
      // Set default or if already in policy, retrieve current values from
      //   privacyCompliancePolicy.compliancePurgeEntities
      return getWithDefault(
        this, 'schemaFieldNamesMappedToDataTypes', []
      ).map(({ fieldName: identifierField, dataType }) => {
        // Get the current classification list
        const currentClassLookup = get(this, 'fieldNameToClass');
        const classification = currentClassLookup[identifierField];

        // If the classification type exists, then find the identifierField, and
        //   assign to field, otherwise null
        const field = classification ?
          get(this, `${sourceClassificationKey}.${classification}`)
            .findBy('identifierField', identifierField) :
          null;

        // Extract the logicalType from the field
        const logicalType = isPresent(field) ? field.logicalType : null;

        // Map to a new literal containing these props
        return {
          dataType,
          identifierField,
          classification,
          logicalType
        };
      });
    }
  ),

  /**
   * @type {Boolean} cached boolean flag indicating that fields do contain a `kafka type`
   *    tracking header.
   *    Used to indicate to viewer that these fields are hidden.
   */
  containsHiddenTrackingFields: computed(
    'classificationDataFieldsSansHiddenTracking.length',
    function () {
      // If their is a diff in complianceDataFields and complianceDataFieldsSansHiddenTracking,
      //   then we have hidden tracking fields
      return get(this, 'classificationDataFieldsSansHiddenTracking.length') !== get(this, 'classificationDataFields.length');
    }),

  /**
   * @type {Array.<Object>} Filters the mapped confidential data fields without `kafka type`
   *   tracking headers
   */
  classificationDataFieldsSansHiddenTracking: computed('classificationDataFields.[]', function () {
    return get(this, 'classificationDataFields')
      .filter(({ identifierField }) => !isTrackingHeaderField(identifierField));
  }),

  /**
   * TODO: DSS-6672 Extract to notifications service
   * Helper method to update user when an async server update to the
   * security specification is handled.
   * @param {XMLHttpRequest|Promise|jqXHR|*} request the server request
   * @param {String} [successMessage] optional _message for successful response
   */
  whenRequestCompletes(request, { successMessage } = {}) {
    Promise.resolve(request)
      .then(({ status = 'error' }) => {
        // The server api currently responds with an object containing
        //   a status when complete
        return status === 'ok' ?
          setProperties(this, {
            _message: successMessage || successUpdating,
            _alertType: 'success'
          }) :
          Promise.reject(new Error(`Reason code for this is ${status}`));
      })
      .catch(err => {
        let _message = `${failedUpdating} \n ${err}`;
        let _alertType = 'danger';

        if (get(this, 'isNewSecuritySpecification')) {
          _message = 'This dataset does not have any ' +
            'previously saved fields with a Security Classification.';
          _alertType = 'info';
        }

        setProperties(this, {
          _message,
          _alertType
        });
      });
  },

  /**
   * TODO:DSS-6719 refactor into mixin
   * Clears recently shown user messages
   */
  clearMessages() {
    return setProperties(this, {
      _message: '',
      _alertType: ''
    });
  },

  /**
   * Takes an identifierField and a logicalType and updates the field on the
   * classification if it exists. Otherwise this is a no-op
   * @param {String} identifierField
   * @param {String} logicalType the type to be updated
   */
  changeFieldLogicalType(identifierField, logicalType) {
    // The current classification name for the candidate identifier
    const currentClassLookup = get(this, 'fieldNameToClass');
    const currentClassificationName = currentClassLookup[identifierField];
    // The current classification list
    const sourceClassification = get(this, `${sourceClassificationKey}.${currentClassificationName}`);

    if (!Array.isArray(sourceClassification)) {
      throw new Error(`
      You have specified a classification object that is not a list ${sourceClassification}.
      Ensure that the classification for this identifierField (${identifierField}) is
      set before attempting to change the logicalType.
      `);
    }

    const field = sourceClassification.findBy('identifierField', identifierField);
    // Clone. `field` attributes should be scalar, otherwise use a deepClone
    //   algo.
    const localField = Object.assign({}, field, { logicalType });

    // Clone the current list without the identifierField to be updated
    const previousClassification = sourceClassification.filter(
      ({ identifierField: fieldName }) => fieldName !== identifierField
    );

    // concat newly updated field to old classification list
    const updatedClassification = [localField, ...previousClassification];

    // Reset current classification list
    return sourceClassification.setObjects([...updatedClassification]);
  },

  /**
   * Checks that each field in a given source classification has a value
   *   for the logicalType
   * @param {Array} sourceClassification
   */
  ensureFieldsContainLogicalType: (sourceClassification = []) =>
    sourceClassification.every(({ logicalType }) => logicalType),

  actions: {
    /**
     * Updates the logical type for a field with the provided identifierField
     * @param {String} identifierField the name of the field to update
     * @param {String} logicalType the updated logical type
     * @return {*|String|void}
     */
    updateLogicalType({ identifierField }, { value: logicalType }) {
      // TODO:DSS-6719 refactor into mixin
      this.clearMessages();
      return this.changeFieldLogicalType(identifierField, logicalType);
    },
    /**
     * Toggles the provided identifierField onto a classification list
     *   on securitySpecification.classification, identified by the provided
     *   classKey.
     * @param {String} identifierField field on the dataset
     * @param {String} classKey the name of the class to add, or potentially
     *   remove the identifierField from
     */
    updateClassification({ identifierField }, { value: classKey }) {
      // fieldNames can be paths i.e. identifierField.identifierPath.subPath
      //   therefore, using Ember's `path lookup` syntax will not work
      const currentClassLookup = get(this, 'fieldNameToClass');
      const currentClass = currentClassLookup[identifierField];

      // TODO:DSS-6719 refactor into mixin
      this.clearMessages();
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
        currentClassification.setObjects(
          currentClassification.filter(
            ({ identifierField: fieldName }) => fieldName !== identifierField)
        );
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
        classification.addObject({ identifierField });
      }
    },

    /**
     * Notify controller to propagate changes
     * @return {Boolean}
     */
    saveSecuritySpecification() {
      /**
       * For Each classifier ensure that the fields on the securitySpec
       *   contain a valid `logicalType` attribute
       * @type {Boolean}
       */
      const classedFieldsHaveLogicalType = classifiers.every(classifier =>
        this.ensureFieldsContainLogicalType(
          getWithDefault(this, `${sourceClassificationKey}.${classifier}`, [])
        ));

      if (classedFieldsHaveLogicalType) {
        this.whenRequestCompletes(get(this, 'onSave')());
      } else {
        setProperties(this, {
          _message: missingTypes,
          _alertType: 'danger'
        });
      }

      return false;
    },

    /**
     * Rolls back changes made to the compliance spec to current
     * server state
     */
    resetSecuritySpecification() {
      const options = {
        successMessage: 'Field classification has been reset to the previously saved state.'
      };
      this.whenRequestCompletes(get(this, 'onReset')(), options);
    }
  }
});
