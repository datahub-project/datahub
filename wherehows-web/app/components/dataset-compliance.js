import Ember from 'ember';
import isTrackingHeaderField from 'wherehows-web/utils/validators/tracking-headers';
import {
  classifiers,
  DatasetClassifiers,
  fieldIdentifierTypes,
  fieldIdentifierOptions,
  fieldIdentifierTypeIds,
  idLogicalTypes,
  nonIdFieldLogicalTypes,
  defaultFieldDataTypeClassification,
  compliancePolicyStrings,
  logicalTypesForIds,
  logicalTypesForGeneric,
  hasPredefinedFieldFormat,
  getDefaultLogicalType,
  complianceSteps,
  hiddenTrackingFields,
  isExempt
} from 'wherehows-web/constants';
import {
  isPolicyExpectedShape,
  fieldChangeSetRequiresReview,
  mergeMappedColumnFieldsWithSuggestions
} from 'wherehows-web/utils/datasets/compliance-policy';
import scrollMonitor from 'scrollmonitor';
import { hasEnumerableKeys } from 'wherehows-web/utils/object';
import { arrayFilter, isListUnique } from 'wherehows-web/utils/array';
import noop from 'wherehows-web/utils/noop';

const {
  Component,
  computed,
  computed: { gt },
  set,
  get,
  run,
  setProperties,
  getProperties,
  getWithDefault,
  String: { classify },
  inject: { service }
} = Ember;

const { schedule } = run;
const {
  complianceDataException,
  missingTypes,
  successUpdating,
  failedUpdating,
  helpText,
  successUploading,
  invalidPolicyData
} = compliancePolicyStrings;

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
 * String constant referencing the datasetClassification on the privacy policy
 * @type {String}
 */
const datasetClassificationKey = 'complianceInfo.datasetClassification';
/**
 * A list of available keys for the datasetClassification map on the security specification
 * @type {Array<keyof typeof DatasetClassifiers>}
 */
const datasetClassifiersKeys = Object.keys(DatasetClassifiers);

/**
 * A reference to the compliance policy entities on the complianceInfo map
 * @type {string}
 */
const policyComplianceEntitiesKey = 'complianceInfo.complianceEntities';

/**
 * Returns a list of changeSet fields that requires user attention
 * @type {function({}): Array<{ isDirty, suggestion, privacyPolicyExists, suggestionAuthority }>}
 */
const changeSetFieldsRequiringReview = arrayFilter(fieldChangeSetRequiresReview);

/**
 * The initial state of the compliance step for a zero based array
 * @type {number}
 */
const initialStepIndex = -1;

export default Component.extend({
  sortColumnWithName: 'identifierField',
  filterBy: 'identifierField',
  sortDirection: 'asc',
  searchTerm: '',
  helpText,
  fieldIdentifierOptions,
  hiddenTrackingFields,

  /**
   * Tracks the current index of the
   * @type {number}
   */
  editStepIndex: initialStepIndex,
  /**
   * Converts the hash of complianceSteps to a list of steps
   * @type {Array<{}>}
   */
  editSteps: Object.keys(complianceSteps)
    .sort()
    .map(key => complianceSteps[key]),

  /**
   * Handles the transition between steps in the compliance edit wizard
   * including performing each step's post processing action once a user has
   * completed a step
   * @type {Ember.ComputedProperty}
   * @returns {object} the editStep
   * TODO: improve ergonomics by enabling async awareness in the templates
   * visually, step transition happens before the post step action is actually completed,
   * even though this is reversed if the object is rejected
   */
  editStep: computed(
    'editStepIndex',
    (function() {
      // initialize the previous action with a no-op function
      let previousAction = noop;
      // initialize the last seen index to the same value as editStepIndex
      let lastIndex = initialStepIndex;

      return function() {
        const currentIndex = get(this, 'editStepIndex');
        // the current step in the edit sequence
        const editStep = this.editSteps[currentIndex] || {};
        const { name } = editStep;

        if (name) {
          // using the steps name, construct a reference to the step process handler
          const nextAction = this.actions[`did${classify(name)}`];
          let previousActionResult;

          // if the transition is backward, then the previous action is ignored
          currentIndex > lastIndex && (previousActionResult = previousAction.call(this));
          lastIndex = currentIndex;

          Promise.resolve(previousActionResult)
            .then(() => {
              // if the previous action is resolved successfully, then replace with the next processor
              if (typeof nextAction === 'function') {
                return (previousAction = nextAction);
              }
              previousAction = noop;
            })
            .catch(() => {
              // if the previous action settles in a rejected state, replace with no-op before
              // invoking the previousStep action to go back in the sequence
              // batch previousStep invocation in a afterRender queue due to editStepIndex update
              previousAction = noop;
              run(() => {
                if (this.isDestroyed || this.isDestroyed) {
                  return;
                }
                schedule('afterRender', this, this.actions.previousStep);
              });
            });
        }

        return editStep;
      };
    })()
  ),

  classNames: ['compliance-container'],
  classNameBindings: ['isEditing:compliance-container--edit-mode'],

  /**
   * Flag indicating that the component is in edit mode
   * @type {Ember.ComputedProperty}
   * @return {boolean}
   */
  isEditing: gt('editStepIndex', initialStepIndex),

  /**
   * Convenience flag indicating the policy is not currently being edited
   * @type {Ember.computed}
   * @return {boolean}
   */
  isReadOnly: computed.not('isEditing'),

  /**
   * Flag indicating that the component is currently saving / attempting to save the privacy policy
   * @type {String}
   */
  isSaving: false,

  /**
   * Returns a list of ui values and labels for review filter drop-down
   * @type {Ember.ComputedProperty<{value: string, label:string}>}
   */
  fieldReviewOptions: computed(function() {
    return [
      { value: 'showAll', label: 'Showing all fields' },
      {
        value: 'showReview',
        label: 'Showing only fields to review'
      }
    ];
  }),

  /**
   * Default to show all fields to review
   * @type {string}
   */
  fieldReviewOption: 'showAll',

  /**
   * Reference to the application notifications Service
   * @type {Ember.Service}
   */
  notifications: service(),

  didReceiveAttrs() {
    this._super(...Array.from(arguments));
    this.resetEdit();
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
   * Resets the editable state of the component, dependent on `isNewComplianceInfo` flag
   */
  resetEdit() {
    return get(this, 'isNewComplianceInfo') ? this.updateStep(0) : this.updateStep(initialStepIndex);
  },

  /**
   * Ensure that props received from on this component
   * are valid, otherwise flag
   */
  validateAttrs() {
    const fieldNames = getWithDefault(this, 'schemaFieldNamesMappedToDataTypes', []).mapBy('fieldName');

    // identifier field names from the column api should be unique
    if (isListUnique(fieldNames.sort())) {
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
   * @type {Ember.ComputedProperty} Filters the mapped compliance data fields without `kafka type`
   *   tracking headers
   * @return {Array<object>}
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
   * Checks if any of the attributes on the dataset classification is false
   * @type {Ember.ComputedProperty}
   * @return {boolean}
   */
  excludesSomeMemberData: computed(datasetClassificationKey, function() {
    const sourceDatasetClassification = get(this, datasetClassificationKey) || {};

    return Object.values(sourceDatasetClassification).some(hasMemberData => !hasMemberData);
  }),

  /**
   * Determines if all member data fields should be shown in the member data table i.e. show only fields contained in
   * this dataset or otherwise
   */
  shouldShowAllMemberData: computed.or('showAllDatasetMemberData', 'isEditing'),

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

    return datasetClassifiersKeys.sort().reduce((classifiers, classifier) => {
      return [
        ...classifiers,
        {
          classifier,
          value: sourceDatasetClassification[classifier],
          label: DatasetClassifiers[classifier]
        }
      ];
    }, []);
  }),

  /**
   *
   * @param {Array<object>} columnFieldProps
   * @param {Array<object>} complianceEntities
   * @param {policyModificationTime}
   * @return {object}
   */
  mapColumnIdFieldsToCurrentPrivacyPolicy(columnFieldProps, complianceEntities, { policyModificationTime }) {
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

    return columnFieldProps.reduce((acc, { identifierField, dataType }) => {
      const currentPrivacyAttrs = getKeysOnField(
        ['identifierType', 'logicalType', 'securityClassification'],
        identifierField,
        complianceEntities
      );

      return {
        ...acc,
        [identifierField]: {
          identifierField,
          dataType,
          ...currentPrivacyAttrs,
          policyModificationTime,
          privacyPolicyExists: hasEnumerableKeys(currentPrivacyAttrs),
          isDirty: false
        }
      };
    }, {});
  },

  /**
   * Computed prop over the current Id fields in the Privacy Policy
   * @type {Ember.computed}
   */
  columnIdFieldsToCurrentPrivacyPolicy: computed(
    `{truncatedColumnFields,${policyComplianceEntitiesKey}.[]}`,
    function() {
      // Truncated list of Dataset field names and data types currently returned from the column endpoint
      const columnFieldProps = get(this, 'truncatedColumnFields').map(({ fieldName, dataType }) => ({
        identifierField: fieldName,
        dataType
      }));
      // Dataset fields that currently have a compliance policy
      const currentComplianceEntities = get(this, policyComplianceEntitiesKey) || [];

      return this.mapColumnIdFieldsToCurrentPrivacyPolicy(columnFieldProps, currentComplianceEntities, {
        policyModificationTime: getWithDefault(this, 'complianceInfo.modifiedTime', 0)
      });
    }
  ),

  /**
   * Caches a reference to the generated list of merged data between the column api and the current compliance entities list
   * @type {Array<{identifierType: string, logicalType: string, securityClassification: string, privacyPolicyExists: boolean, isDirty: boolean, [suggestion]: object}>}
   */
  compliancePolicyChangeSet: computed('columnIdFieldsToCurrentPrivacyPolicy', function() {
    // truncatedColumnFields is a dependency for cp columnIdFieldsToCurrentPrivacyPolicy, so no need to dep on that directly
    return mergeMappedColumnFieldsWithSuggestions(
      get(this, 'columnIdFieldsToCurrentPrivacyPolicy'),
      get(this, 'identifierFieldToSuggestion')
    );
  }),

  /**
   * Returns a list of changeSet fields that meets the user selected filter criteria
   * @type {Ember.computed}
   * @return {Array<{}>}
   */
  filteredChangeSet: computed('changeSetReviewCount', 'fieldReviewOption', 'compliancePolicyChangeSet', function() {
    const changeSet = get(this, 'compliancePolicyChangeSet');

    return get(this, 'fieldReviewOption') === 'showReview' ? changeSetFieldsRequiringReview(changeSet) : changeSet;
  }),

  /**
   * Returns a count of changeSet fields that require user attention
   * @type {Ember.computed}
   * @return {Array<{}>}
   */
  changeSetReviewCount: computed(
    'compliancePolicyChangeSet.@each.{isDirty,suggestion,privacyPolicyExists,suggestionAuthority}',
    function() {
      return changeSetFieldsRequiringReview(get(this, 'compliancePolicyChangeSet')).length;
    }
  ),

  /**
   * Creates a mapping of compliance suggestions to identifierField
   * This improves performance in a subsequent merge op since this loop
   * happens only once and is cached
   * @type {object}
   */
  identifierFieldToSuggestion: computed('complianceSuggestion', function() {
    const identifierFieldToSuggestion = {};
    const complianceSuggestion = get(this, 'complianceSuggestion') || {};
    const { lastModified: suggestionsModificationTime, suggestedFieldClassification = [] } = complianceSuggestion;

    // If the compliance suggestions array contains suggestions the create reduced lookup map,
    // otherwise, ignore
    if (suggestedFieldClassification.length) {
      return suggestedFieldClassification.reduce(
        (
          identifierFieldToSuggestion,
          { suggestion: { identifierField, identifierType, logicalType, securityClassification }, confidenceLevel }
        ) => ({
          ...identifierFieldToSuggestion,
          [identifierField]: {
            identifierType,
            logicalType,
            securityClassification,
            confidenceLevel,
            suggestionsModificationTime
          }
        }),
        identifierFieldToSuggestion
      );
    }

    return identifierFieldToSuggestion;
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
   * Helper method to update user when an async server update to the
   * security specification is handled.
   * @param {Promise|*} request the server request
   * @param {String} [successMessage] optional message for successful response
   * @param { Boolean} [isSaving = false] optional flag indicating when the user intends to persist / save
   */
  whenRequestCompletes(request, { successMessage, isSaving = false } = {}) {
    const notify = get(this, 'notifications.notify');

    return Promise.resolve(request)
      .then(({ status = 'error' }) => {
        return status === 'ok'
          ? notify('success', { content: successMessage || successUpdating })
          : Promise.reject(new Error(`Reason code for this is ${status}`));
      })
      .catch(err => {
        let message = `${failedUpdating} \n ${err}`;

        if (get(this, 'isNewComplianceInfo') && !isSaving) {
          return notify('info', {
            content: 'This dataset does not have any previously saved fields with a identifying information.'
          });
        }

        notify('error', { content: message });
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

    defaultTypeClassification =
      identifierType === fieldIdentifierTypes.enterpriseAccount.value
        ? defaultFieldDataTypeClassification['ID']
        : defaultTypeClassification;
    this.actions.onFieldClassificationChange.call(this, { identifierField }, { value: defaultTypeClassification });
  },

  /**
   * Requires that the user confirm that any non-id fields are ok to be saved without a field format specified
   * @return {Boolean}
   */
  async confirmUnformattedFields() {
    // Current list of compliance entities on policy
    const complianceEntities = get(this, policyComplianceEntitiesKey);
    // All candidate fields that can be on policy, excluding tracking type fields
    const datasetFields = get(
      this,
      'compliancePolicyChangeSet'
    ).map(({ identifierField, identifierType, logicalType, classification }) => ({
      identifierField,
      identifierType,
      logicalType,
      securityClassification: classification
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

    const dialogActions = {};
    let isConfirmed = true;
    let unformattedComplianceEntities = [];

    // If there are unformatted fields, require confirmation from user
    if (unformatted.length) {
      unformattedComplianceEntities = unformatted.map(({ identifierField }) => ({
        identifierField,
        identifierType: fieldIdentifierTypes.none.value,
        logicalType: null,
        securityClassification: null
      }));

      const confirmHandler = (function() {
        return new Promise((resolve, reject) => {
          dialogActions['didConfirm'] = () => resolve();
          dialogActions['didDismiss'] = () => reject();
        });
      })();

      // Create confirmation dialog
      get(this, 'notifications').notify('confirm', {
        header: 'Some field formats are unspecified',
        content:
          `There are ${unformatted.length} non-ID fields that have no field format specified. ` +
          `Are you sure they don't contain any of the following PII?\n\n` +
          `Name, Email, Phone, Address, Location, IP Address, Payment Info, Password, National ID, Device ID etc.`,
        dialogActions: dialogActions
      });

      try {
        await confirmHandler;
      } catch (e) {
        isConfirmed = false;
      }
    }

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
    const notify = get(this, 'notifications.notify');
    const complianceEntities = get(this, policyComplianceEntitiesKey);
    const idFieldsHaveValidLogicalType = this.checkEachEntityByLogicalType(
      complianceEntities.filter(({ identifierType }) => fieldIdentifierTypeIds.includes(identifierType)),
      [...genericLogicalTypes, ...idLogicalTypes]
    );
    const fieldIdentifiersAreUnique = isListUnique(complianceEntities.mapBy('identifierField'));
    const schemaFieldLengthGreaterThanComplianceEntities = this.isSchemaFieldLengthGreaterThanComplianceEntities();

    if (!fieldIdentifiersAreUnique || !schemaFieldLengthGreaterThanComplianceEntities) {
      notify('error', { content: complianceDataException });
      return Promise.reject(new Error(complianceDataException));
    }

    if (!idFieldsHaveValidLogicalType) {
      return Promise.reject(notify('error', { content: missingTypes }));
    }
  },

  /**
   * Gets a reference to the current dataset classification object
   */
  getDatasetClassificationRef() {
    let sourceDatasetClassification = getWithDefault(this, datasetClassificationKey, {});

    // For datasets initially without a datasetClassification, the default value is null
    if (sourceDatasetClassification === null) {
      sourceDatasetClassification = set(this, datasetClassificationKey, {});
    }

    return sourceDatasetClassification;
  },

  /**
   * Display a modal dialog requesting that the user check affirm that the purge type is exempt
   * @return {Promise<void>}
   */
  showPurgeExemptionWarning() {
    const dialogActions = {};

    get(this, 'notifications').notify('confirm', {
      header: 'Confirm purge exemption',
      content:
        'By choosing this option you understand that either Legal or HSEC may contact you to verify the purge exemption',
      dialogActions
    });

    return new Promise((resolve, reject) => {
      dialogActions['didConfirm'] = () => resolve();
      dialogActions['didDismiss'] = () => reject();
    });
  },

  /**
   * Updates the currently active step in the edit sequence
   * @param {number} step
   */
  updateStep(step) {
    set(this, 'editStepIndex', step);
  },

  actions: {
    /**
     * Sets each datasetClassification value as false
     */
    async markDatasetAsNotContainingMemberData() {
      const dialogActions = {};
      const confirmMarkAllHandler = new Promise((resolve, reject) => {
        dialogActions.didDismiss = () => reject();
        dialogActions.didConfirm = () => resolve();
      });
      let willMarkAllAsNo = true;

      get(this, 'notifications').notify('confirm', {
        content: 'Are you sure that any this dataset does not contain any of the listed types of member data?',
        header: 'Dataset contains no member data',
        dialogActions
      });

      try {
        await confirmMarkAllHandler;
      } catch (e) {
        willMarkAllAsNo = false;
      }

      return (
        willMarkAllAsNo &&
        setProperties(
          this.getDatasetClassificationRef(),
          datasetClassifiersKeys.reduce(
            (classification, classifier) => ({ ...classification, ...{ [classifier]: false } }),
            {}
          )
        )
      );
    },

    /**
     * Toggles the flag to show all member potential member data fields that may be contained in this dataset
     */
    onShowAllDatasetMemberData() {
      return this.toggleProperty('showAllDatasetMemberData');
    },

    /**
     * Updates the fieldReviewOption with the user selected value
     * @param {string} value
     */
    onFieldReviewChange({ value }) {
      return set(this, 'fieldReviewOption', value);
    },

    /**
     * Progresses 1 step backward in the edit sequence
     */
    previousStep() {
      const editStepIndex = get(this, 'editStepIndex');
      const nextState = editStepIndex > 0 ? editStepIndex - 1 : editStepIndex;
      this.updateStep(nextState);
    },

    /**
     * Progresses 1 step forward in the edit sequence
     */
    nextStep() {
      const { editStepIndex, editSteps } = getProperties(this, ['editStepIndex', 'editSteps']);
      const nextState = editStepIndex < editSteps.length - 1 ? editStepIndex + 1 : editStepIndex;
      this.updateStep(nextState);
    },

    /**
     * Handler for setting the dataset classification into edit mode and rendering into DOM
     */
    async didEditCompliancePolicy() {
      const isConfirmed = await this.confirmUnformattedFields();

      if (isConfirmed) {
        // Ensure that the fields on the policy meet the validation criteria before proceeding
        // Otherwise exit early
        try {
          await this.validateFields();
        } catch (e) {
          // Flag this dataset's data as problematic
          if (e instanceof Error && e.message === complianceDataException) {
            set(this, '_hasBadData', true);
            window.scrollTo(0, 0);
          }

          // return;
          throw e;
        }

        // If user provides confirmation for unformatted fields or there are none,
        // then validate fields against expectations
        // otherwise inform user of validation exception
        // setProperties(this, { isEditingCompliancePolicy: false, isEditingDatasetClassification: true });
      } else {
        throw new Error('unConfirmedUnformattedFields');
      }

      return isConfirmed;
    },

    /**
     * Handles post processing tasks after the purge policy step has been completed
     * @return {void|Promise.<void>}
     */
    didEditPurgePolicy() {
      if (isExempt(get(this, 'complianceInfo.complianceType'))) {
        return this.showPurgeExemptionWarning();
      }
    },

    /**
     * Augments the field props with w a suggestionAuthority indicating that the field
     * suggestion has either been accepted or ignored, and assigns the value of that change to the prop
     * @param {object} field field for which this suggestion intent should apply
     * @param {string | void} [intent] user's intended action for suggestion, Defaults to `ignore`
     */
    onFieldSuggestionIntentChange(field, intent = 'ignore') {
      set(field, 'suggestionAuthority', intent);
    },

    /**
     * Receives the json representation for compliance and applies each key to the policy
     * @param {string} textString string representation for the JSON file
     */
    onComplianceJsonUpload(textString) {
      let policy;
      try {
        policy = JSON.parse(textString);
      } catch (e) {
        get(this, 'notifications').notify('error', {
          content: invalidPolicyData
        });
      }

      if (isPolicyExpectedShape(policy)) {
        setProperties(this, {
          'complianceInfo.complianceEntities': policy.complianceEntities,
          'complianceInfo.datasetClassification': policy.datasetClassification
        });

        get(this, 'notifications').notify('info', {
          content: successUploading
        });
      }

      get(this, 'notifications').notify('error', {
        content: invalidPolicyData
      });
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
      const currentComplianceEntities = get(this, 'compliancePolicyChangeSet');
      // A reference to the current field in the compliance list, it should exist even for empty complianceEntities
      // since this is a reference created in the working copy: compliancePolicyChangeSet
      const currentFieldInComplianceList = currentComplianceEntities.findBy('identifierField', identifierField);
      let logicalType;
      if (hasPredefinedFieldFormat(identifierType)) {
        logicalType = getDefaultLogicalType(identifierType);
      }

      setProperties(currentFieldInComplianceList, {
        identifierType,
        logicalType,
        isDirty: true
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
        setProperties(field, { logicalType, isDirty: true });
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
      const currentFieldInComplianceList = get(this, 'compliancePolicyChangeSet').findBy(
        'identifierField',
        identifierField
      );
      // TODO:DSS-6719 refactor into mixin
      this.clearMessages();

      // Apply the updated classification value to the current instance of the field in working copy
      setProperties(currentFieldInComplianceList, { classification, isDirty: true });
    },

    /**
     * Updates the source object representing the current datasetClassification map
     * @param {String} classifier the property on the datasetClassification to update
     * @param {Boolean} value flag indicating if this dataset contains member data for the specified classifier
     * @return {*}
     */
    onChangeDatasetClassification(classifier, value) {
      return set(this.getDatasetClassificationRef(), classifier, value);
    },

    /**
     * Updates the complianceType on the compliance policy
     * @param {PurgePolicy} purgePolicy
     */
    onDatasetPurgePolicyChange(purgePolicy) {
      // directly set the complianceType to the updated value
      return set(this, 'complianceInfo.complianceType', purgePolicy);
    },

    /**
     * If all validity checks are passed, invoke onSave action on controller
     */
    async saveCompliance() {
      const setSaveFlag = (flag = false) => set(this, 'isSaving', flag);

      try {
        const isSaving = true;
        const onSave = get(this, 'onSave');
        setSaveFlag(isSaving);

        return await this.whenRequestCompletes(onSave(), { isSaving });
      } finally {
        setSaveFlag();
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
