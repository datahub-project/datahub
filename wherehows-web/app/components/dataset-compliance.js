import Ember from 'ember';
import isTrackingHeaderField from 'wherehows-web/utils/validators/tracking-headers';
import {
  classifiers,
  datasetClassifiers,
  fieldIdentifierTypes,
  idLogicalTypes,
  genericLogicalTypes,
  defaultFieldDataTypeClassification
} from 'wherehows-web/constants';

const {
  Component,
  computed,
  set,
  get,
  Observable: { mixins: { notifyPropertyChange } },
  setProperties,
  getWithDefault,
  String: { htmlSafe }
} = Ember;

// TODO: DSS-6671 Extract to constants module
const missingTypes = 'Looks like some fields may contain privacy data but do not have a specified `Field Format`?';
const successUpdating = 'Your changes have been successfully saved!';
const failedUpdating = 'Oops! We are having trouble updating this dataset at the moment.';
const hiddenTrackingFieldsMsg = htmlSafe(
  '<p>Some fields in this dataset have been hidden from the table(s) below. ' +
    "These are tracking fields for which we've been able to predetermine the compliance classification.</p>" +
    '<p>For example: <code>header.memberId</code>, <code>requestHeader</code>. ' +
    'Hopefully, this saves you some scrolling!</p>'
);
const helpText = {
  classification: 'For each of the fields below, please classify them based on the data handling table found at go/dht. ' +
    'The default classification should suffice in most cases.',
  subjectOwner: 'A field can contain one or multiple member IDs that do not technically "own" the entire record, ' +
    'e.g. the list of recipients of a message. The field should be marked as a "Subject Owner" in this case.'
};

/**
 * Quick regex to check if a string ends with urn case-insensitive, cached outside scope to avoid recreating
 * on every function invocation
 * @type {RegExp}
 */
const endsWithUrnRegex = /.*urn/i;

/**
 * Takes a string, returns a formatted string. Niche , single use case
 * for now, so no need to make into a helper
 * @param {String} string
 */
const formatAsCapitalizedStringWithSpaces = string => string.replace(/[A-Z]/g, match => ` ${match}`).capitalize();

/**
 * String constant referencing the datasetClassification on the privacy policy
 * @type {String}
 */
const datasetClassificationKey = 'complianceInfo.datasetClassification';
/**
 * A list of available keys for the datasetClassification map on the security specification
 * @type {Array}
 */
const datasetClassifiersKeys = Object.keys(datasetClassifiers);

/**
 * String constant identifying the classified fields on the security spec
 * @type {String}
 */
const policyFieldClassificationKey = 'complianceInfo.fieldClassification';

/**
 * A reference to the compliance policy entities on the complianceInfo map
 * @type {string}
 */
const policyComplianceEntitiesKey = 'complianceInfo.complianceEntities';
/**
 * Duplicate check using every to short-circuit iteration
 * @param {Array} names = [] the list to check for dupes
 * @return {Boolean} true is unique, false otherwise
 */
const fieldNamesAreUnique = (names = []) => names.every((name, index) => names.indexOf(name) === index);

/**
 * A list of field identifier types mapped to label, value options for select display
 * @type {any[]|Array.<{value: String, label: String}>}
 */
const fieldIdentifierOptions = Object.keys(fieldIdentifierTypes).map(fieldIdentifierType => ({
  value: fieldIdentifierTypes[fieldIdentifierType].value,
  label: fieldIdentifierTypes[fieldIdentifierType].displayAs
}));

/**
 * A list of field identifier types that are Ids i.e member ID, org ID, group ID
 * @type {any[]|Array.<String>}
 */
const fieldIdentifierTypeIds = Object.keys(fieldIdentifierTypes)
  .map(fieldIdentifierType => fieldIdentifierTypes[fieldIdentifierType])
  .filter(({ isId }) => isId)
  .mapBy('value');

/**
 * Caches a list of logicalType mappings for displaying its value and a label by logicalType
 * @param {String} logicalType
 */
const cachedLogicalTypes = logicalType =>
  computed(function() {
    return {
      id: idLogicalTypes,
      generic: genericLogicalTypes
    }[logicalType].map(value => ({
      value,
      label: value
        ? value.replace(/_/g, ' ').replace(/([A-Z]{3,})/g, f => f.toLowerCase().capitalize())
        : 'Select Format'
    }));
  });

export default Component.extend({
  sortColumnWithName: 'identifierField',
  filterBy: 'identifierField',
  sortDirection: 'asc',
  searchTerm: '',
  helpText,
  fieldIdentifierOptions,
  hiddenTrackingFields: hiddenTrackingFieldsMsg,

  didReceiveAttrs() {
    this._super(...arguments);
    // Perform validation step on the received component attributes
    this.validateAttrs();
  },

  /**
   * Ensure that props received from on this component
   * are valid, otherwise flag
   */
  validateAttrs() {
    const fieldNames = getWithDefault(this, 'schemaFieldNamesMappedToDataTypes', []).mapBy('fieldName');

    if (fieldNamesAreUnique(fieldNames.sort())) {
      return set(this, '_hasBadData', false);
    }

    // Flag this component's data as problematic
    set(this, '_hasBadData', true);
  },

  // Map logicalTypes to options consumable by DOM
  idLogicalTypes: cachedLogicalTypes('id'),

  // Map generic logical type to options consumable in DOM
  genericLogicalTypes: cachedLogicalTypes('generic'),

  // Map classifiers to options better consumed in DOM
  classifiers: ['', ...classifiers.sort()].map(value => ({
    value,
    label: value ? formatAsCapitalizedStringWithSpaces(value) : '...'
  })),

  /**
   * @type {Boolean} cached boolean flag indicating that fields do contain a `kafka type`
   *    tracking header.
   *    Used to indicate to viewer that these fields are hidden.
   */
  containsHiddenTrackingFields: computed('complianceDataFieldsSansHiddenTracking.length', function() {
    // If their is a diff in complianceDataFields and complianceDataFieldsSansHiddenTracking,
    //   then we have hidden tracking fields
    return get(this, 'complianceDataFieldsSansHiddenTracking.length') !== get(this, 'complianceDataFields.length');
  }),

  /**
   * @type {Array.<Object>} Filters the mapped compliance data fields without `kafka type`
   *   tracking headers
   */
  complianceDataFieldsSansHiddenTracking: computed('complianceDataFields.[]', function() {
    return get(this, 'complianceDataFields').filter(({ identifierField }) => !isTrackingHeaderField(identifierField));
  }),

  /**
   * Checks that all tags/ dataset content types have a boolean value
   * @type {Ember.computed}
   */
  isDatasetFullyClassified: computed('datasetClassification', function() {
    const datasetClassification = get(this, 'datasetClassification');

    return Object.keys(datasetClassification)
      .map(key => ({ value: datasetClassification[key].value }))
      .every(({ value }) => [true, false].includes(value));
  }),

  /**
   * Computed property that is dependent on all the keys in the datasetClassification map
   *   Returns a new map of datasetClassificationKey: String-> Object.<Boolean|undefined,String>
   * @type {Ember.computed}
   */
  datasetClassification: computed(`${datasetClassificationKey}.{${datasetClassifiersKeys.join(',')}}`, function() {
    const sourceDatasetClassification = get(this, datasetClassificationKey) || {};

    return Object.keys(datasetClassifiers).reduce((datasetClassification, classifier) => {
      return Object.assign({}, datasetClassification, {
        [classifier]: {
          value: sourceDatasetClassification[classifier],
          label: datasetClassifiers[classifier]
        }
      });
    }, {});
  }),

  /**
   * Lists all dataset fields found in the `columns` performs an intersection
   * of fields with the currently persisted and/or updated
   * privacyCompliancePolicy.complianceEntities.
   * The returned list is a map of fields with current or default privacy properties
   */
  complianceDataFields: computed(
    `${policyComplianceEntitiesKey}.@each.identifierType`,
    `${policyComplianceEntitiesKey}.[]`,
    policyFieldClassificationKey,
    'schemaFieldNamesMappedToDataTypes',
    function() {
      /**
       * Retrieves an attribute on the `policyComplianceEntitiesKey` where the identifierField is the same as the
       *   provided field name
       * @param attribute
       * @param fieldName
       * @return {null}
       */
      const getAttributeOnField = (attribute, fieldName) => {
        const complianceEntities = get(this, policyComplianceEntitiesKey) || [];
        const sourceField = complianceEntities.find(
          ({ identifierField }) => identifierField === fieldName
        );
        return sourceField ? sourceField[attribute] : null;
      };

      /**
       * Get value for a list of attributes
       * @param {Array} attributes list of attribute keys to pull from
       *   sourceField
       * @param {String} fieldName name of the field to lookup
       * @return {Array} list of attribute values
       */
      const getAttributesOnField = (attributes = [], fieldName) =>
        attributes.map(attr => getAttributeOnField(attr, fieldName));

      // Set default or if already in policy, retrieve current values from
      //   privacyCompliancePolicy.complianceEntities
      return getWithDefault(
        this,
        'schemaFieldNamesMappedToDataTypes',
        []
      ).map(({ fieldName: identifierField, dataType }) => {
        const [identifierType, isSubject, logicalType] = getAttributesOnField(
          ['identifierType', 'isSubject', 'logicalType'],
          identifierField
        );
        // Runtime converts the identifierType to subjectMember if the isSubject flag is true
        const computedIdentifierType = identifierType === fieldIdentifierTypes.member.value && isSubject
          ? fieldIdentifierTypes.subjectMember.value
          : identifierType;
        const idLogicalTypes = get(this, 'idLogicalTypes');
        // Filtered list of id logical types that end with urn, or have no value
        const urnFieldFormats = idLogicalTypes.filter(
          type => String(type.value).match(endsWithUrnRegex) || !type.value
        );
        // Get the current classification list
        const fieldClassification = get(this, policyFieldClassificationKey) || {};
        // The field formats applicable to the current identifierType
        let fieldFormats = fieldIdentifierTypeIds.includes(identifierType)
          ? idLogicalTypes
          : get(this, 'genericLogicalTypes');
        fieldFormats = identifierType === fieldIdentifierTypes.generic.value ? urnFieldFormats : fieldFormats;

        return {
          dataType,
          identifierField,
          fieldFormats,
          identifierType: computedIdentifierType,
          classification: fieldClassification[identifierField],
          // Same object reference for equality comparision
          logicalType: fieldFormats.findBy('value', logicalType)
        };
      });
    }
  ),

  /**
   * Checks that each entity in sourceEntities has a valid logicalType
   * ie. logical Type exists is the idLogicalTypes or genericLogicalTypes
   * @param {Ember.Array} sourceEntities complianceEntities
   * @return {Boolean|*} Contains or does not
   */
  ensureIdTypesHaveLogicalType: sourceEntities => {
    const logicalTypesInUppercase = [...idLogicalTypes, ...genericLogicalTypes].map(type => type.toUpperCase());

    return sourceEntities.every(entity => logicalTypesInUppercase.includes(get(entity, 'logicalType')));
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
   * TODO: DSS-6672 Extract to notifications service
   * Helper method to update user when an async server update to the
   * security specification is handled.
   * @param {Promise|*} request the server request
   * @param {String} [successMessage] optional _message for successful response
   * @param { Boolean} [isSaving = false] optional flag indicating when the user intends to persist / save
   */
  whenRequestCompletes(request, { successMessage, isSaving = false } = {}) {
    Promise.resolve(request)
      .then(({ status = 'error' }) => {
        return status === 'ok'
          ? setProperties(this, {
              _message: successMessage || successUpdating,
              _alertType: 'success'
            })
          : Promise.reject(new Error(`Reason code for this is ${status}`));
      })
      .catch(err => {
        let _message = `${failedUpdating} \n ${err}`;
        let _alertType = 'danger';

        if (get(this, 'isNewComplianceInfo') && !isSaving) {
          _message = 'This dataset does not have any previously saved fields with a identifying information.';
          _alertType = 'info';
        }

        setProperties(this, {
          _message,
          _alertType
        });
      });
  },

  /**
   * Sets the default classification for the given identifier field
   * @param {String} identifierField
   * @param {String} logicalType
   */
  setDefaultClassification({ identifierField }, { value: logicalType = '' } = {}) {
    const defaultTypeClassification = defaultFieldDataTypeClassification[logicalType] || null;
    this.actions.onFieldClassificationChange.call(this, { identifierField }, { value: defaultTypeClassification });
  },

  actions: {
    /**
     * When a user updates the identifierFieldType in the DOM, update the backing store
     * @param identifierField
     * @param logicalType
     * @param identifierType
     */
    onFieldIdentifierTypeChange({ identifierField, logicalType }, { value: identifierType }) {
      const complianceList = get(this, policyComplianceEntitiesKey);
      // A reference to the current field in the compliance list if it exists
      const currentFieldInComplianceList = complianceList.findBy('identifierField', identifierField);
      const subjectId = fieldIdentifierTypes.subjectMember.value;
      const updatedEntity = Object.assign({}, currentFieldInComplianceList, {
        identifierField,
        identifierType: subjectId === identifierType ? fieldIdentifierTypes.member.value : identifierType,
        isSubject: subjectId === identifierType ? true : null,
        logicalType: !currentFieldInComplianceList ? logicalType : void 0
      });
      let transientComplianceList = complianceList;

      if (currentFieldInComplianceList) {
        transientComplianceList = complianceList.filter(
          ({ identifierField: fieldName }) => fieldName !== identifierField
        );
      }

      complianceList.setObjects([updatedEntity, ...transientComplianceList]);
      this.setDefaultClassification({ identifierField });
    },

    /**
     * Updates the logical type for the given identifierField
     * @param {Object} field
     * @prop {String} field.identifierField
     * @param {Event} e the DOM change event
     * @return {*}
     */
    onFieldLogicalTypeChange(field, e) {
      const { identifierField } = field;
      const { value: logicalType } = e || {};
      let sourceIdentifierField = get(this, policyComplianceEntitiesKey).findBy('identifierField', identifierField);

      // If the identifierField does not current exist, invoke onFieldIdentifierChange to add it on the compliance list
      if (!sourceIdentifierField) {
        this.actions.onFieldIdentifierTypeChange.call(
          this,
          {
            identifierField,
            logicalType
          },
          { value: fieldIdentifierTypes.none.value }
        );
      }

      set(sourceIdentifierField, 'logicalType', logicalType);

      return this.setDefaultClassification({ identifierField }, { value: logicalType });
    },

    /**
     * Updates the filed classification
     * @param {String} identifierField the identifier field to update the classification for
     * @param {String} classification
     * @return {*}
     */
    onFieldClassificationChange({ identifierField }, { value: classification = null }) {
      let fieldClassification = get(this, policyFieldClassificationKey);
      // For datasets initially without a fieldClassification, the default value is null
      if (fieldClassification === null) {
        fieldClassification = set(this, policyFieldClassificationKey, {});
      }

      // TODO:DSS-6719 refactor into mixin
      this.clearMessages();

      if (!classification && identifierField in fieldClassification) {
        const updatedFieldClassification = Object.assign({}, fieldClassification);
        delete updatedFieldClassification[identifierField];
        set(this, policyFieldClassificationKey, updatedFieldClassification);
        return;
      }

      set(fieldClassification, identifierField, classification);
      // Ember.computedProperties don't seem to have a direct way of depending on the shape of an object changing at
      //  runtime, notifyPropertyChange forces the computed property recompute the object reference
      return notifyPropertyChange.call(this, policyFieldClassificationKey);
    },

    /**
     * Updates the source object representing the current datasetClassification map
     * @param {String} classifier the property on the datasetClassification to update
     * @param {Boolean} value flag indicating if this dataset contains member data for the specified classifier
     */
    onChangeDatasetClassification(classifier, value) {
      let sourceDatasetClassification = getWithDefault(this, datasetClassificationKey, {});

      // For datasets initially without a datasetClassification, the default value is null
      if (sourceDatasetClassification === null) {
        sourceDatasetClassification = set(this, datasetClassificationKey, {});
      }

      return set(sourceDatasetClassification, classifier, value);
    },

    /**
     * If all validity checks are passed, invoke onSave action on controller
     */
    saveCompliance() {
      const complianceList = get(this, policyComplianceEntitiesKey);
      const idFieldsHaveValidLogicalType = this.ensureIdTypesHaveLogicalType(
        complianceList.filter(({ identifierType }) => fieldIdentifierTypeIds.includes(identifierType))
      );

      if (idFieldsHaveValidLogicalType) {
        return this.whenRequestCompletes(get(this, 'onSave')(), { isSaving: true });
      }

      setProperties(this, {
        _message: missingTypes,
        _alertType: 'danger'
      });
    },

    // Rolls back changes made to the compliance spec to current
    // server state
    resetCompliance() {
      const options = {
        successMessage: 'Field classification has been reset to the previously saved state.'
      };
      this.whenRequestCompletes(get(this, 'onReset')(), options);
    }
  }
});
