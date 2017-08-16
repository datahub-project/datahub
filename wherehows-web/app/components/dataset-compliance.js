import Ember from 'ember';
import isTrackingHeaderField from 'wherehows-web/utils/validators/tracking-headers';
import {
  classifiers,
  datasetClassifiers,
  fieldIdentifierTypes,
  idLogicalTypes,
  nonIdFieldLogicalTypes,
  defaultFieldDataTypeClassification,
  compliancePolicyStrings
} from 'wherehows-web/constants';
import { isPolicyExpectedShape } from 'wherehows-web/utils/datasets/functions';
import scrollMonitor from 'scrollmonitor';

const {
  assert,
  Component,
  computed,
  set,
  get,
  setProperties,
  getProperties,
  getWithDefault,
  String: { htmlSafe }
} = Ember;

const { complianceDataException, missingTypes, successUpdating, failedUpdating, helpText } = compliancePolicyStrings;

const hiddenTrackingFieldsMsg = htmlSafe(
  '<p>Some fields in this dataset have been hidden from the table(s) below. ' +
    "These are tracking fields for which we've been able to predetermine the compliance classification.</p>" +
    '<p>For example: <code>header.memberId</code>, <code>requestHeader</code>. ' +
    'Hopefully, this saves you some scrolling!</p>'
);

/**
 * Takes a string, returns a formatted string. Niche , single use case
 * for now, so no need to make into a helper
 * @param {String} string
 */
const formatAsCapitalizedStringWithSpaces = string => string.replace(/[A-Z]/g, match => ` ${match}`).capitalize();

/**
 * List of non Id field data type classifications
 * @type {Array}
 */
const genericLogicalTypes = Object.keys(nonIdFieldLogicalTypes).sort();

/**
 * Merged object of logicalTypes
 * @type {Object}
 */
const logicalTypes = Object.assign({}, nonIdFieldLogicalTypes);
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
 * A reference to the compliance policy entities on the complianceInfo map
 * @type {string}
 */
const policyComplianceEntitiesKey = 'complianceInfo.complianceEntities';
/**
 * Duplicate check using every to short-circuit iteration
 * @param {Array} list = [] the list to check for dupes
 * @return {Boolean} true is unique, false otherwise
 */
const listIsUnique = (list = []) => new Set(list).size === list.length;

assert('`fieldIdentifierTypes` contains an object with a key `none`', typeof fieldIdentifierTypes.none === 'object');
const fieldIdentifierTypeKeysBarNone = Object.keys(fieldIdentifierTypes).filter(k => k !== 'none');
const fieldDisplayKeys = ['none', '_', ...fieldIdentifierTypeKeysBarNone];

/**
 * A list of field identifier types mapped to label, value options for select display
 * @type {any[]|Array.<{value: String, label: String}>}
 */
const fieldIdentifierOptions = fieldDisplayKeys.map(fieldIdentifierType => {
  const divider = '──────────';
  const { value = fieldIdentifierType, displayAs: label = divider } = fieldIdentifierTypes[fieldIdentifierType] || {};

  // Adds a divider for a value of _
  // Visually this separates ID from none fieldIdentifierTypes
  return {
    value,
    label,
    isDisabled: fieldIdentifierType === '_'
  };
});

/**
 * A list of field identifier types that are Ids i.e member ID, org ID, group ID
 * @type {any[]|Array.<String>}
 */
export const fieldIdentifierTypeIds = Object.keys(fieldIdentifierTypes)
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
      label: logicalTypes[value]
        ? logicalTypes[value].displayAs
        : value.replace(/_/g, ' ').replace(/([A-Z]{3,})/g, f => f.toLowerCase().capitalize())
    }));
  });

// Map logicalTypes to options consumable by DOM
export const logicalTypesForIds = cachedLogicalTypes('id');

// Map generic logical type to options consumable in DOM
export const logicalTypesForGeneric = cachedLogicalTypes('generic');

export default Component.extend({
  sortColumnWithName: 'identifierField',
  filterBy: 'identifierField',
  sortDirection: 'asc',
  searchTerm: '',
  helpText,
  fieldIdentifierOptions,
  hiddenTrackingFields: hiddenTrackingFieldsMsg,
  classNames: ['compliance-container'],
  classNameBindings: ['isEditing:compliance-container--edit-mode'],
  /**
   * Flag indicating that the component is in edit mode
   * @type {String}
   */
  isEditing: false,
  /**
   * Flag indicating that the component is currently saving / attempting to save the privacy policy
   * @type {String}
   */
  isSaving: false,

  didReceiveAttrs() {
    this._super(...arguments);
    // If a compliance policy does not exist for this dataset, place it in edit mode by default
    set(this, 'isEditing', get(this, 'isNewComplianceInfo'));
    // Perform validation step on the received component attributes
    this.validateAttrs();
  },

  /**
   * @override
   */
  didRender() {
    this._super(...arguments);
    // Hides DOM elements that are not currently visible in the UI and unhides them once the user scrolls the
    // elements into view
    this.enableDomCloaking();
  },

  /**
   * A `lite` / intermediary step to occlusion culling, this helps to improve the rendering of
   * elements that are currently rendered in the viewport by hiding that aren't.
   * Setting them to visibility hidden doesn't remove them from the document flow, but the browser
   * doesn't have to deal with layout for the affected elements since they are off-screen
   */
  enableDomCloaking() {
    const [dom] = this.$('.dataset-compliance-fields');
    const triggerCount = 100;
    if (dom) {
      const rows = dom.querySelectorAll('tbody tr');

      // if we already have watchers for elements, or if the elements previously cached are no longer valid,
      // e.g. those elements were destroyed when new data was received, pagination etc
      if (rows.length > triggerCount && (!this.complianceWatchers || !this.complianceWatchers.has(rows[0]))) {
        /**
         * If an item is not in the viewport add a class to occlude it
         */
        const cloaker = function() {
          if (!this.isInViewport) {
            return this.watchItem.classList.add('compliance-row--off-screen');
          }
          this.watchItem.classList.remove('compliance-row--off-screen');
        };
        this.watchers = [];

        // Retain a weak reference to DOM nodes
        this.complianceWatchers = new WeakMap(
          [...rows].map(row => {
            const watcher = scrollMonitor.create(row);
            watcher['stateChange'](cloaker);
            cloaker.call(watcher);
            this.watchers = [...this.watchers, watcher];

            return [watcher.watchItem, watcher];
          })
        );
      }
    }
  },

  /**
   * Cleans up the artifacts from the dom cloaking operation, drops references held by scroll monitor
   */
  disableDomCloaking() {
    if (!this.watchers || !Array.isArray(this.watchers)) {
      return;
    }

    this.watchers.forEach(watcher => watcher.destroy());
  },

  /**
   * @override
   */
  willDestroyElement() {
    this.disableDomCloaking();
  },

  /**
   * Ensure that props received from on this component
   * are valid, otherwise flag
   */
  validateAttrs() {
    const fieldNames = getWithDefault(this, 'schemaFieldNamesMappedToDataTypes', []).mapBy('fieldName');

    // identifier field names from the column api should be unique
    if (listIsUnique(fieldNames.sort())) {
      return set(this, '_hasBadData', false);
    }

    // Flag this component's data as problematic
    set(this, '_hasBadData', true);
  },

  // Map logicalTypes to options consumable by DOM
  idLogicalTypes: logicalTypesForIds,

  // Map generic logical type to options consumable in DOM
  genericLogicalTypes: logicalTypesForGeneric,

  // Map classifiers to options better consumed in DOM
  classifiers: ['', ...classifiers.sort()].map(value => ({
    value,
    label: value ? formatAsCapitalizedStringWithSpaces(value) : '...'
  })),

  /**
   * Caches the policy's modification time in milliseconds
   */
  policyModificationTimeInEpoch: computed('complianceInfo', function() {
    return getWithDefault(this, 'complianceInfo.modifiedTime', 0) * 1000;
  }),

  /**
   * @type {Boolean} cached boolean flag indicating that fields do contain a `kafka type`
   *    tracking header.
   *    Used to indicate to viewer that these fields are hidden.
   */
  containsHiddenTrackingFields: computed('truncatedColumnFields.length', function() {
    // If their is a diff in schemaFieldNamesMappedToDataTypes and truncatedColumnFields,
    //   then we have hidden tracking fields
    return get(this, 'truncatedColumnFields.length') !== get(this, 'schemaFieldNamesMappedToDataTypes.length');
  }),

  /**
   * @type {Array.<Object>} Filters the mapped compliance data fields without `kafka type`
   *   tracking headers
   */
  truncatedColumnFields: computed('schemaFieldNamesMappedToDataTypes', function() {
    return getWithDefault(this, 'schemaFieldNamesMappedToDataTypes', []).filter(
      ({ fieldName }) => !isTrackingHeaderField(fieldName)
    );
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
   * Determines if the save feature is allowed for the current dataset, otherwise e.g. interface should be disabled
   * @type {Ember.computed}
   */
  isSavingDisabled: computed('isDatasetFullyClassified', 'isSaving', function() {
    const { isDatasetFullyClassified, isSaving } = getProperties(this, ['isDatasetFullyClassified', 'isSaving']);

    return !isDatasetFullyClassified || isSaving;
  }),

  /**
   * Checks to ensure the the number of fields added to compliance entities is less than or equal
   * to what is available on the dataset schema
   * @return {boolean}
   */
  isSchemaFieldLengthGreaterThanComplianceEntities() {
    const { length: columnFieldsLength } = getWithDefault(this, 'schemaFieldNamesMappedToDataTypes', []);
    const { length: complianceListLength } = get(this, policyComplianceEntitiesKey);

    return columnFieldsLength >= complianceListLength;
  },

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
  mergeComplianceEntitiesAndColumnFields(columnIdFieldsToCurrentPrivacyPolicy = {}, truncatedColumnFields = []) {
    return truncatedColumnFields.map(({ fieldName: identifierField, dataType }) => {
      const {
        [identifierField]: { identifierType, logicalType, securityClassification }
      } = columnIdFieldsToCurrentPrivacyPolicy;

      return {
        identifierField,
        dataType,
        identifierType,
        logicalType,
        classification: securityClassification
      };
    });
  },

  /**
   *
   * @param {Array} columnFieldNames
   * @return {*|{}|any}
   */
  mapColumnIdFieldsToCurrentPrivacyPolicy(columnFieldNames) {
    const complianceEntities = get(this, policyComplianceEntitiesKey) || [];
    const getKeysOnField = (keys = [], fieldName, source = []) => {
      const sourceField = source.find(({ identifierField }) => identifierField === fieldName) || {};
      let ret = {};

      for (const [key, value] of Object.entries(sourceField)) {
        if (keys.includes(key)) {
          ret = { ...ret, [key]: value };
        }
      }
      return ret;
    };

    return columnFieldNames.reduce((acc, identifierField) => {
      const currentPrivacyAttrs = getKeysOnField(
        ['identifierType', 'logicalType', 'securityClassification'],
        identifierField,
        complianceEntities
      );

      return { ...acc, ...{ [identifierField]: currentPrivacyAttrs } };
    }, {});
  },

  /**
   * Computed prop over the current Id fields in the Privacy Policy
   * @type {Ember.computed}
   */
  columnIdFieldsToCurrentPrivacyPolicy: computed(
    'truncatedColumnFields',
    `${policyComplianceEntitiesKey}.[]`,
    function() {
      const columnFieldNames = get(this, 'truncatedColumnFields').map(({ fieldName }) => fieldName);
      return this.mapColumnIdFieldsToCurrentPrivacyPolicy(columnFieldNames);
    }
  ),

  /**
   * Caches a reference to the generated list of merged data between the column api and the current compliance entities list
   * @type {Ember.computed}
   */
  mergedComplianceEntitiesAndColumnFields: computed('columnIdFieldsToCurrentPrivacyPolicy', function() {
    // truncatedColumnFields is a dependency for cp columnIdFieldsToCurrentPrivacyPolicy, so no need to dep on that directly
    return this.mergeComplianceEntitiesAndColumnFields(
      get(this, 'columnIdFieldsToCurrentPrivacyPolicy'),
      get(this, 'truncatedColumnFields')
    );
  }),

  /**
   * Checks that each entity in sourceEntities has a generic
   * @param {Array} sourceEntities = [] the source entities to be matched against
   * @param {Array} logicalTypes = [] list of logicalTypes to check against
   */
  checkEachEntityByLogicalType: (sourceEntities = [], logicalTypes = []) =>
    sourceEntities.every(
      ({ logicalType }) =>
        typeof logicalType === 'object' ? logicalTypes.includes(logicalType.value) : logicalTypes.includes(logicalType)
    ),

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
    return Promise.resolve(request)
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
   * Using the provided logicalType, or in some cases the identifierType, determines the fields
   * default security classification based on a lookup
   * @param {String} identifierField the field for which the default classification should apply
   * @param {String} [identifierType] the current identifier type for the field
   * @param {String} [logicalType] the logicalType / (field format) for the identifierField
   */
  setDefaultClassification({ identifierField, identifierType }, { value: logicalType = '' } = {}) {
    let defaultTypeClassification = defaultFieldDataTypeClassification[logicalType] || null;
    // If the identifierType is of custom, set the default classification to the of a CUSTOM_ID, otherwise use value
    // based on logicalType
    defaultTypeClassification =
      identifierType === fieldIdentifierTypes.custom.value
        ? defaultFieldDataTypeClassification['CUSTOM_ID']
        : defaultTypeClassification;
    this.actions.onFieldClassificationChange.call(this, { identifierField }, { value: defaultTypeClassification });
  },

  /**
   * Requires that the user confirm that any non-id fields are ok to be saved without a field format specified
   * @return {Boolean}
   */
  confirmUnformattedFields() {
    // Current list of compliance entities on policy
    const complianceEntities = get(this, policyComplianceEntitiesKey);
    // All candidate fields that can be on policy, excluding tracking type fields
    const datasetFields = get(
      this,
      'mergedComplianceEntitiesAndColumnFields'
    ).map(({ identifierField, identifierType, isSubject, logicalType }) => ({
      identifierField,
      identifierType,
      isSubject,
      logicalType
    }));
    // Fields that do not have a logicalType, and no identifierType or identifierType is `fieldIdentifierTypes.none`
    const { formatted, unformatted } = datasetFields.reduce(
      ({ formatted, unformatted }, field) => {
        const { identifierType, logicalType } = getProperties(field, ['identifierType', 'logicalType']);
        if (!logicalType && (fieldIdentifierTypes.none.value === identifierType || !identifierType)) {
          unformatted = [...unformatted, field];
        } else {
          formatted = [...formatted, field];
        }
        return { formatted, unformatted };
      },
      { formatted: [], unformatted: [] }
    );
    let isConfirmed = true;
    let unformattedComplianceEntities = [];

    // If there are unformatted fields, require confirmation from user
    if (unformatted.length) {
      unformattedComplianceEntities = unformatted.map(({ identifierField }) => ({
        identifierField,
        identifierType: fieldIdentifierTypes.none.value,
        isSubject: null,
        logicalType: void 0
      }));

      isConfirmed = confirm(
        `There are ${unformatted.length} non-ID fields that have no field format specified. ` +
          `Are you sure they don't contain any of the following PII?\n\n` +
          `Name, Email, Phone, Address, Location, IP Address, Payment Info, Password, National ID, Device ID etc.`
      );
    }
    // If the user confirms that this is ok, apply the unformatted fields on the current compliance list
    //   to be saved
    isConfirmed && complianceEntities.setObjects([...formatted, ...unformattedComplianceEntities]);

    return isConfirmed;
  },

  /**
   * Ensures the fields in the updated list of compliance entities meet the criteria
   * checked in the function. If criteria is not met, an the returned promise is settled
   * in a rejected state, otherwise fulfilled
   * @method
   * @return {any | Promise<any>}
   */
  validateFields() {
    const complianceEntities = get(this, policyComplianceEntitiesKey);
    const idFieldsHaveValidLogicalType = this.checkEachEntityByLogicalType(
      complianceEntities.filter(({ identifierType }) => fieldIdentifierTypeIds.includes(identifierType)),
      [...genericLogicalTypes, ...idLogicalTypes]
    );
    const fieldIdentifiersAreUnique = listIsUnique(complianceEntities.mapBy('identifierField'));
    const schemaFieldLengthGreaterThanComplianceEntities = this.isSchemaFieldLengthGreaterThanComplianceEntities();

    //
    if (!fieldIdentifiersAreUnique || !schemaFieldLengthGreaterThanComplianceEntities) {
      return Promise.reject(new Error(complianceDataException));
    }

    if (!idFieldsHaveValidLogicalType) {
      return Promise.reject(
        setProperties(this, {
          _message: missingTypes,
          _alertType: 'danger'
        })
      );
    }
  },

  actions: {
    /**
     * Handle the user intent to place this compliance component in edit mode
     */
    onEdit() {
      set(this, 'isEditing', true);
    },

    /**
     * Receives the json representation for compliance and applies each key to the policy
     * @param {String} textString string representation for the JSON file
     */
    onComplianceJsonUpload(textString) {
      const policy = JSON.parse(textString);
      if (isPolicyExpectedShape(policy)) {
        set(this, 'complianceInfo', policy);

        // If all is good, then we can saveCompliance so user does not have to manually click
        return this.actions.saveCompliance();
      }

      alert('Received policy in an unexpected format! Please check the provided attributes and try again.');
    },

    /**
     * Handles the compliance policy download action
     */
    onComplianceDownloadJson() {
      const currentPolicy = get(this, 'complianceInfo');
      const policyProps = [datasetClassificationKey, policyComplianceEntitiesKey].map(name => name.split('.').pop());
      const policy = Object.assign({}, getProperties(currentPolicy, policyProps));
      const href = `data:text/json;charset=utf-8,${encodeURIComponent(JSON.stringify(policy))}`;
      const download = `${get(this, 'datasetName')}_policy.json`;
      const anchor = document.createElement('a');
      const anchorParent = document.body;

      /**
       *  Post download housekeeping
       */
      const cleanupPostDownload = () => {
        anchor.removeEventListener('click', cleanupPostDownload);
        anchorParent.removeChild(anchor);
      };

      Object.assign(anchor, { download, href });
      anchor.addEventListener('click', cleanupPostDownload);

      // Element needs to be in DOM to receive event in firefox
      anchorParent.appendChild(anchor);

      anchor.click();
    },

    /**
     * When a user updates the identifierFieldType in the DOM, update the backing store
     * @param {String} identifierField
     * @param {String} logicalType
     * @param {String} identifierType
     */
    onFieldIdentifierTypeChange({ identifierField }, { value: identifierType }) {
      const currentComplianceEntities = get(this, 'mergedComplianceEntitiesAndColumnFields');
      // A reference to the current field in the compliance list, it should exist even for empty complianceEntities
      // since this is a reference created in the working copy: mergedComplianceEntitiesAndColumnFields
      const currentFieldInComplianceList = currentComplianceEntities.findBy('identifierField', identifierField);
      setProperties(currentFieldInComplianceList, {
        identifierType,
        logicalType: void 0
      });
      // Set the defaultClassification for the identifierField,
      // although the classification is based on the logicalType,
      // an identifierField may only have one valid logicalType for it's given identifierType
      this.setDefaultClassification({ identifierField, identifierType });
    },

    /**
     * Updates the logical type for the given identifierField
     * @param {Object} field
     * @prop {String} field.identifierField
     * @param {Event} e the DOM change event
     * @return {*}
     */
    onFieldLogicalTypeChange(field, { value: logicalType } = {}) {
      const { identifierField } = field;

      // If the identifierField does not current exist, invoke onFieldIdentifierChange to add it on the compliance list
      if (!field) {
        this.actions.onFieldIdentifierTypeChange.call(
          this,
          { identifierField, logicalType },
          { value: fieldIdentifierTypes.none.value }
        );
      } else {
        set(field, 'logicalType', logicalType);
      }

      return this.setDefaultClassification({ identifierField }, { value: logicalType });
    },

    /**
     * Updates the filed classification
     * @param {String} identifierField the identifier field to update the classification for
     * @param {String} classification
     * @return {*}
     */
    onFieldClassificationChange({ identifierField }, { value: classification = null }) {
      const currentFieldInComplianceList = get(this, 'mergedComplianceEntitiesAndColumnFields').findBy(
        'identifierField',
        identifierField
      );
      // TODO:DSS-6719 refactor into mixin
      this.clearMessages();

      // Apply the updated classification value to the current instance of the field in working copy
      set(currentFieldInComplianceList, 'classification', classification);
    },

    /**
     * Updates the source object representing the current datasetClassification map
     * @param {String} classifier the property on the datasetClassification to update
     * @param {Boolean} value flag indicating if this dataset contains member data for the specified classifier
     * @return {*}
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
    async saveCompliance() {
      const setSaveFlag = (flag = false) => set(this, 'isSaving', flag);
      // If fields are confirmed as unique we can proceed with saving compliance entities
      const saveConfirmed = this.confirmUnformattedFields();

      // If user provides confirmation for unformatted fields or there are none,
      // then validate fields against expectations
      // otherwise inform user of validation exception
      if (saveConfirmed) {
        try {
          const isSaving = true;
          const onSave = get(this, 'onSave');
          setSaveFlag(isSaving);
          await this.validateFields();

          return await this.whenRequestCompletes(onSave(), { isSaving });
        } catch (e) {
          // Flag this dataset's data as problematic
          if (e instanceof Error && e.message === complianceDataException) {
            set(this, '_hasBadData', true);
            window.scrollTo(0, 0);
          }
        } finally {
          setSaveFlag();
        }
      }
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
