import Component from '@ember/component';
import { computed, set, get, setProperties, getProperties, getWithDefault, observer } from '@ember/object';
import ComputedProperty, { gt, not, or } from '@ember/object/computed';
import { run, schedule } from '@ember/runloop';
import { inject } from '@ember/service';
import { classify } from '@ember/string';
import { IFieldIdentifierOption, ISecurityClassificationOption } from 'wherehows-web/constants/dataset-compliance';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { IDataPlatform } from 'wherehows-web/typings/api/list/platforms';
import { readPlatforms } from 'wherehows-web/utils/api/list/platforms';

import {
  getSecurityClassificationDropDownOptions,
  DatasetClassifiers,
  getFieldIdentifierOptions,
  getDefaultSecurityClassification,
  compliancePolicyStrings,
  getComplianceSteps,
  hiddenTrackingFields,
  isExempt,
  ComplianceFieldIdValue,
  IComplianceFieldIdentifierOption,
  IDatasetClassificationOption,
  DatasetClassification,
  SuggestionIntent,
  PurgePolicy,
  getSupportedPurgePolicies
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
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';
import Notifications, { NotificationEvent, IConfirmOptions } from 'wherehows-web/services/notifications';
import { IDatasetColumn } from 'wherehows-web/typings/api/datasets/columns';
import {
  IComplianceInfo,
  IComplianceEntity,
  ISuggestedFieldClassification,
  IComplianceSuggestion
} from 'wherehows-web/typings/api/datasets/compliance';
import { ApiStatus } from 'wherehows-web/utils/api';
import { task, TaskInstance } from 'ember-concurrency';

/**
 * Describes the DatasetCompliance actions index signature to allow
 * access to actions using `did${editStepName}` accessors
 */
interface IDatasetComplianceActions {
  didEditCompliancePolicy: () => Promise<boolean>;
  didEditPurgePolicy: () => Promise<{} | void>;
  didEditDatasetLevelCompliancePolicy: () => Promise<void>;
  [K: string]: (...args: Array<any>) => any;
}

/**
 * Alias for the properties defined on an object indicating the values for a compliance entity object in
 * addition to related component metadata using in processing ui interactions / rendering for the field
 */
type SchemaFieldToPolicyValue = Pick<
  IComplianceEntity,
  'identifierField' | 'identifierType' | 'logicalType' | 'securityClassification' | 'nonOwner'
> & {
  privacyPolicyExists: boolean;
  isDirty: boolean;
  policyModificationTime: IComplianceInfo['modifiedTime'];
  dataType: string;
};

/**
 * Describes the interface for a mapping of field names to type, SchemaFieldToPolicyValue
 * @interface ISchemaFieldsToPolicy
 */
interface ISchemaFieldsToPolicy {
  [fieldName: string]: SchemaFieldToPolicyValue;
}

/**
 * Alias for the properties on an object indicating the suggested values for field / record properties
 * as well as suggestions metadata
 */
type SchemaFieldToSuggestedValue = Pick<
  IComplianceEntity,
  'identifierType' | 'logicalType' | 'securityClassification'
> &
  Pick<ISuggestedFieldClassification, 'confidenceLevel'> & {
    suggestionsModificationTime: IComplianceSuggestion['lastModified'];
  };

/**
 * Describes the mapping of attributes to value types for a datasets schema field names to suggested property values
 * @interface ISchemaFieldsToSuggested
 */
interface ISchemaFieldsToSuggested {
  [fieldName: string]: SchemaFieldToSuggestedValue;
}
/**
 * Describes the interface for a locally assembled compliance field instance
 * used in rendering a compliance row
 */
export type IComplianceChangeSet = {
  suggestion?: SchemaFieldToSuggestedValue;
  suggestionAuthority?: SuggestionIntent;
} & SchemaFieldToPolicyValue;

/**
 * Defines the applicable string values for compliance fields drop down filter
 */
type ShowAllShowReview = 'showReview' | 'showAll';

const {
  complianceDataException,
  complianceFieldNotUnique,
  missingTypes,
  successUpdating,
  failedUpdating,
  helpText,
  successUploading,
  invalidPolicyData,
  missingPurgePolicy,
  missingDatasetSecurityClassification
} = compliancePolicyStrings;

/**
 * Takes a list of compliance data types and maps a list of compliance id's with idType set to true
 * @param {Array<IComplianceDataType>} [complianceDataTypes=[]] the list of compliance data types to transform
 * @return {Array<ComplianceFieldIdValue>}
 */
const getIdTypeDataTypes = (complianceDataTypes: Array<IComplianceDataType> = []) =>
  complianceDataTypes.filter(complianceDataType => complianceDataType.idType).mapBy('id');

/**
 * String constant referencing the datasetClassification on the privacy policy
 * @type {string}
 */
const datasetClassificationKey = 'complianceInfo.datasetClassification';
/**
 * A list of available keys for the datasetClassification map on the security specification
 * @type {Array<keyof typeof DatasetClassifiers>}
 */
const datasetClassifiersKeys = <Array<keyof typeof DatasetClassifiers>>Object.keys(DatasetClassifiers);

/**
 * A reference to the compliance policy entities on the complianceInfo map
 * @type {string}
 */
const policyComplianceEntitiesKey = 'complianceInfo.complianceEntities';

/**
 * Returns a list of changeSet fields that requires user attention
 * @type {function({}): Array<{ isDirty, suggestion, privacyPolicyExists, suggestionAuthority }>}
 */
const changeSetFieldsRequiringReview = arrayFilter<IComplianceChangeSet>(fieldChangeSetRequiresReview);

/**
 * The initial state of the compliance step for a zero based array
 * @type {number}
 */
const initialStepIndex = -1;

/**
 * Defines observers for the DatasetCompliance Component
 * @type {Component}
 */
const ObservableDecorator = Component.extend({
  /**
   * Observes changes editStepIndex to trigger the update edit step task
   * @type {() => void}
   */
  editStepIndexChanged: observer('editStepIndex', function(this: DatasetCompliance) {
    // @ts-ignore ts limitation with the ember object model, fixed in ember 3.1 with es5 getters
    get(this, 'updateEditStepTask').perform();
  }),

  /**
   * Observes changes to the platform property and invokes the task to update the supportedPurgePolicies prop
   * @type {() => void}
   */
  platformChanged: observer('platform', function(this: DatasetCompliance) {
    // @ts-ignore ts limitation with the ember object model, fixed in ember 3.1 with es5 getters
    get(this, 'complianceAvailabilityTask').perform();
  })
});

export default class DatasetCompliance extends ObservableDecorator {
  isNewComplianceInfo: boolean;
  datasetName: string;
  sortColumnWithName: string;
  filterBy: string;
  sortDirection: string;
  searchTerm: string;
  helpText = helpText;
  watchers: Array<{ stateChange: (fn: () => void) => void; watchItem: Element; destroy?: Function }>;
  complianceWatchers: WeakMap<Element, {}>;
  _hasBadData: boolean;
  _message: string;
  _alertType: string;
  platform: IDatasetView['platform'];
  isCompliancePolicyAvailable: boolean = false;
  showAllDatasetMemberData: boolean;
  complianceInfo: void | IComplianceInfo;

  /**
   * Suggested values for compliance types e.g. identifier type and/or logical type
   * @type {IComplianceSuggestion | void}
   */
  complianceSuggestion: IComplianceSuggestion | void;

  schemaFieldNamesMappedToDataTypes: Array<Pick<IDatasetColumn, 'dataType' | 'fieldName'>>;
  onReset: <T extends { status: ApiStatus }>() => Promise<T>;
  onSave: <T extends { status: ApiStatus }>() => Promise<T>;

  classNames = ['compliance-container'];

  classNameBindings = ['isEditing:compliance-container--edit-mode'];

  /**
   * Reference to the application notifications Service
   * @type {ComputedProperty<Notifications>}
   */
  notifications: ComputedProperty<Notifications> = inject();

  /**
   * @type {Handlebars.SafeStringStatic}
   * @memberof DatasetCompliance
   */
  hiddenTrackingFields = hiddenTrackingFields;

  /**
   * Flag indicating that the related dataset is schemaless or has a schema
   * @type {boolean}
   * @memberof DatasetCompliance
   */
  schemaless: boolean;
  /**
   * Tracks the current index of the compliance policy update wizard flow
   * @type {number}
   * @memberof DatasetCompliance
   */
  editStepIndex = initialStepIndex;

  /**
   * List of complianceDataType values
   * @type {Array<IComplianceDataType>}
   * @memberof DatasetCompliance
   */
  complianceDataTypes: Array<IComplianceDataType>;

  // Map of classifiers options for drop down
  classifiers: Array<ISecurityClassificationOption> = getSecurityClassificationDropDownOptions();

  /**
   * Default to show all fields to review
   * @type {string}
   * @memberof DatasetCompliance
   */
  fieldReviewOption: 'showReview' | 'showAll' = 'showAll';
  /**
   * Flag indicating that the component is in edit mode
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetCompliance
   */
  isEditing = gt('editStepIndex', initialStepIndex);

  /**
   * Convenience flag indicating the policy is not currently being edited
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetCompliance
   */
  isReadOnly = not('isEditing');

  /**
   * Flag indicating that the component is currently saving / attempting to save the privacy policy
   * @type {boolean}
   * @memberof DatasetCompliance
   */
  isSaving = false;

  /**
   * The list of supported purge policies for the related platform
   * @type {Array<PurgePolicy>}
   * @memberof DatasetCompliance
   */
  supportedPurgePolicies: Array<PurgePolicy> = [];

  constructor() {
    super(...arguments);

    //sets default values for class fields
    this.sortColumnWithName ||
      set<DatasetCompliance, 'sortColumnWithName', string>(this, 'sortColumnWithName', 'identifierField');
    this.filterBy || set<DatasetCompliance, 'filterBy', string>(this, 'filterBy', 'identifierField');
    this.sortDirection || set<DatasetCompliance, 'sortDirection', string>(this, 'sortDirection', 'asc');
    this.searchTerm || set<DatasetCompliance, 'searchTerm', string>(this, 'searchTerm', '');
    this.schemaFieldNamesMappedToDataTypes || (this.schemaFieldNamesMappedToDataTypes = []);
    this.complianceDataTypes || (this.complianceDataTypes = []);
  }

  /**
   * Lists the compliance wizard edit steps based on the datasets schemaless property
   * @memberof DatasetCompliance
   */
  editSteps = computed('schemaless', function(this: DatasetCompliance): Array<{ name: string }> {
    const hasSchema = !getWithDefault(this, 'schemaless', false);
    const steps = getComplianceSteps(hasSchema);

    // Ensure correct step ordering
    return Object.keys(steps)
      .sort()
      .map((key: string) => steps[+key]);
  });

  /**
   * Reads the complianceDataTypes property and transforms into a list of drop down options for the field
   * identifier type
   * @type {ComputedProperty<Array<IComplianceFieldIdentifierOption  | IFieldIdentifierOption<null | 'NONE'>>>}
   */
  complianceFieldIdDropdownOptions = computed('complianceDataTypes', function(
    this: DatasetCompliance
  ): Array<IComplianceFieldIdentifierOption | IFieldIdentifierOption<null | ComplianceFieldIdValue.None>> {
    type NoneAndUnspecifiedOptions = Array<IFieldIdentifierOption<null | ComplianceFieldIdValue.None>>;

    const noneAndUnSpecifiedDropdownOptions: NoneAndUnspecifiedOptions = [
      { value: null, label: 'Select Field Type...', isDisabled: true },
      { value: ComplianceFieldIdValue.None, label: 'None' }
    ];

    return [...noneAndUnSpecifiedDropdownOptions, ...getFieldIdentifierOptions(get(this, 'complianceDataTypes'))];
  });

  /**
   * e-c Task to update the current edit step in the wizard flow.
   * Handles the transitions between steps, including performing each step's
   * post processing action once a user has completed a step, or reverting the step
   * and stepping backward if the post process fails
   * @type {Task<void, (a?: void) => TaskInstance<void>>}
   * @memberof DatasetCompliance
   */
  updateEditStepTask = (function() {
    // initialize the previous action with a no-op function
    let previousAction = noop;
    // initialize the last seen index to the same value as editStepIndex
    let lastIndex = initialStepIndex;

    return task(function*(this: DatasetCompliance): IterableIterator<void> {
      const { editStepIndex: currentIndex, editSteps } = getProperties(this, ['editStepIndex', 'editSteps']);
      // the current step in the edit sequence
      const editStep = editSteps[currentIndex] || { name: '' };
      const { name } = editStep;

      if (name) {
        // using the steps name, construct a reference to the step process handler
        const nextAction = this.actions[`did${classify(name)}`];
        let previousActionResult: void;

        // if the transition is backward, then the previous action is ignored
        currentIndex > lastIndex && (previousActionResult = previousAction.call(this));
        lastIndex = currentIndex;

        try {
          yield previousActionResult;
          // if the previous action is resolved successfully, then replace with the next processor
          previousAction = typeof nextAction === 'function' ? nextAction : noop;

          set(this, 'editStep', editStep);
        } catch {
          // if the previous action settles in a rejected state, replace with no-op before
          // invoking the previousStep action to go back in the sequence
          // batch previousStep invocation in a afterRender queue due to editStepIndex update
          previousAction = noop;
          run(() => {
            if (this.isDestroyed || this.isDestroying) {
              return;
            }
            schedule('afterRender', this, this.actions.previousStep);
          });
        }
      }
    }).enqueue();
  })();

  /**
   * Holds a reference to the current step in the compliance edit wizard flow
   * @type {{ name: string }}
   */
  editStep: { name: string };

  /**
   * A list of ui values and labels for review filter drop-down
   * @type {Array<{value: string, label:string}>}
   * @memberof DatasetCompliance
   */
  fieldReviewOptions: Array<{ value: DatasetCompliance['fieldReviewOption']; label: string }> = [
    { value: 'showAll', label: 'Showing all fields' },
    { value: 'showReview', label: 'Showing only fields to review' }
  ];

  didReceiveAttrs(this: DatasetCompliance) {
    this._super(...arguments);
    // Perform validation step on the received component attributes
    this.validateAttrs();

    // Set the current step to first edit step if compliance policy is new / doesn't exist
    if (get(this, 'isNewComplianceInfo')) {
      this.updateStep(0);
    }
  }

  didInsertElement(this: DatasetCompliance) {
    get(this, 'complianceAvailabilityTask').perform();
  }

  /**
   * @override
   */
  didRender() {
    this._super(...arguments);
    // Hides DOM elements that are not currently visible in the UI and unhides them once the user scrolls the
    // elements into view
    this.enableDomCloaking();
  }

  /**
   * Parent task to determine if a compliance policy can be created or updated for the dataset
   * @type {Task<TaskInstance<Promise<Array<IDataPlatform>>>, () => TaskInstance<TaskInstance<Promise<Array<IDataPlatform>>>>>}
   * @memberof DatasetCompliance
   */
  complianceAvailabilityTask = task(function*(
    this: DatasetCompliance
  ): IterableIterator<TaskInstance<Promise<Array<IDataPlatform>>>> {
    yield get(this, 'getPlatformPoliciesTask').perform();

    const supportedPurgePolicies = get(this, 'supportedPurgePolicies');
    set(this, 'isCompliancePolicyAvailable', !!supportedPurgePolicies.length);
  }).restartable();

  /**
   * Task to retrieve platform policies and set supported policies for the current platform
   * @type {Task<Promise<Array<IDataPlatform>>, () => TaskInstance<Promise<Array<IDataPlatform>>>>}
   * @memberof DatasetCompliance
   */
  getPlatformPoliciesTask = task(function*(this: DatasetCompliance): IterableIterator<Promise<Array<IDataPlatform>>> {
    const platform = get(this, 'platform');

    if (platform) {
      set(this, 'supportedPurgePolicies', getSupportedPurgePolicies(platform, yield readPlatforms()));
    }
  }).restartable();

  /**
   * A `lite` / intermediary step to occlusion culling, this helps to improve the rendering of
   * elements that are currently rendered in the viewport by hiding that aren't.
   * Setting them to visibility hidden doesn't remove them from the document flow, but the browser
   * doesn't have to deal with layout for the affected elements since they are off-screen
   * @memberof DatasetCompliance
   */
  enableDomCloaking() {
    const dom = this.element.querySelector('.dataset-compliance-fields');
    const triggerThreshold = 100;

    if (dom) {
      const rows = dom.querySelectorAll('tbody tr');

      // if we already have watchers for elements, or if the elements previously cached are no longer valid,
      // e.g. those elements were destroyed when new data was received, pagination etc
      if (rows.length > triggerThreshold && (!this.complianceWatchers || !this.complianceWatchers.has(rows[0]))) {
        /**
         * If an item is not in the viewport add a class to occlude it
         */
        const cloaker = function(this: any) {
          return !this.isInViewport
            ? this.watchItem.classList.add('compliance-row--off-screen')
            : this.watchItem.classList.remove('compliance-row--off-screen');
        };
        this.watchers = [];

        const entries = <Array<[Element, object]>>[...rows].map(row => {
          const watcher: { stateChange: (fn: () => void) => void; watchItem: Element } = scrollMonitor.create(row);
          watcher['stateChange'](cloaker);
          cloaker.call(watcher);
          this.watchers = [...this.watchers, watcher];

          return [watcher.watchItem, watcher];
        });

        // Retain a weak reference to DOM nodes
        this.complianceWatchers = new WeakMap<Element, {}>(entries);
      }
    }
  }

  /**
   * Cleans up the artifacts from the dom cloaking operation, drops references held by scroll monitor
   * @returns void
   * @memberof DatasetCompliance
   */
  disableDomCloaking() {
    if (!this.watchers || !Array.isArray(this.watchers)) {
      return;
    }

    this.watchers.forEach(watcher => watcher.destroy && watcher.destroy());
  }

  /**
   * @override
   * @memberof DatasetCompliance
   */
  willDestroyElement() {
    this.disableDomCloaking();
  }

  /**
   * Ensure that props received from on this component
   * are valid, otherwise flag
   * @returns {boolean | void}
   * @memberof DatasetCompliance
   */
  validateAttrs(this: DatasetCompliance): boolean | void {
    const fieldNames: Array<string> = getWithDefault(this, 'schemaFieldNamesMappedToDataTypes', []).mapBy('fieldName');

    // identifier field names from the column api should be unique
    if (isListUnique(fieldNames.sort())) {
      return set(this, '_hasBadData', false);
    }

    // Flag this component's data as problematic
    set(this, '_hasBadData', true);
  }

  /**
   * Checks that all tags/ dataset content types have a boolean value
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetCompliance
   */
  isDatasetFullyClassified = computed('datasetClassification', function(this: DatasetCompliance): boolean {
    const datasetClassification = get(this, 'datasetClassification');

    return datasetClassification
      .map(({ value }) => ({ value: value }))
      .every(({ value }) => typeof value === 'boolean');
  });

  /**
   * Checks if any of the attributes on the dataset classification is false
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetCompliance
   */
  excludesSomeMemberData = computed(datasetClassificationKey, function(this: DatasetCompliance): boolean {
    const { datasetClassification } = get(this, 'complianceInfo') || { datasetClassification: {} };

    return Object.values(datasetClassification).some(hasMemberData => !hasMemberData);
  });

  /**
   * Determines if all member data fields should be shown in the member data table i.e. show only fields contained in
   * this dataset or otherwise
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetCompliance
   */
  shouldShowAllMemberData = or('showAllDatasetMemberData', 'isEditing');

  /**
   * Determines if the save feature is allowed for the current dataset, otherwise e.g. interface should be disabled
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetCompliance
   */
  isSavingDisabled = computed('isDatasetFullyClassified', 'isSaving', function(this: DatasetCompliance): boolean {
    const { isDatasetFullyClassified, isSaving } = getProperties(this, ['isDatasetFullyClassified', 'isSaving']);

    return !isDatasetFullyClassified || isSaving;
  });

  /**
   * Checks to ensure the the number of fields added to compliance entities is less than or equal
   * to what is available on the dataset schema
   * @return {boolean}
   */
  isSchemaFieldLengthGreaterThanComplianceEntities(this: DatasetCompliance): boolean {
    const complianceInfo = get(this, 'complianceInfo');
    if (complianceInfo) {
      const { length: columnFieldsLength } = getWithDefault(this, 'schemaFieldNamesMappedToDataTypes', []);
      const { length: complianceListLength } = get(complianceInfo, 'complianceEntities') || [];

      return columnFieldsLength >= complianceListLength;
    }

    return false;
  }

  /**
   * Computed property that is dependent on all the keys in the datasetClassification map
   * @type {ComputedProperty<Array<IDatasetClassificationOption>>}
   * @memberof DatasetCompliance
   */
  datasetClassification = computed(`${datasetClassificationKey}.{${datasetClassifiersKeys.join(',')}}`, function(
    this: DatasetCompliance
  ): Array<IDatasetClassificationOption> {
    const complianceInfo = get(this, 'complianceInfo');
    if (complianceInfo) {
      const { datasetClassification } = complianceInfo;

      return datasetClassifiersKeys.sort().reduce((datasetClassifiers, classifier) => {
        return [
          ...datasetClassifiers,
          {
            classifier,
            value: datasetClassification[classifier],
            label: DatasetClassifiers[classifier]
          }
        ];
      }, []);
    }

    return [];
  });

  /**
   * @param {Array<{identifierField: string, dataType: string}>} columnFieldProps
   * @param {IComplianceInfo.complianceEntities} complianceEntities
   * @param {(policyModificationTime: string)} { policyModificationTime }
   * @returns {ISchemaFieldsToPolicy}
   * @memberof DatasetCompliance
   */
  mapColumnIdFieldsToCurrentPrivacyPolicy(
    columnFieldProps: Array<{ identifierField: string; dataType: string }>,
    complianceEntities: IComplianceInfo['complianceEntities'],
    { policyModificationTime }: { policyModificationTime: string }
  ): ISchemaFieldsToPolicy {
    const getKeysOnField = <K extends keyof IComplianceEntity>(
      keys: Array<K> = [],
      fieldName: string,
      source: IComplianceInfo['complianceEntities'] = []
    ): { [V in K]: IComplianceEntity[V] } | {} => {
      const sourceField: IComplianceEntity | void = source.find(({ identifierField }) => identifierField === fieldName);
      let ret = {};

      if (sourceField) {
        for (const [key, value] of <Array<[K, IComplianceEntity[K]]>>Object.entries(sourceField)) {
          // includes is not generic, hence narrowing string assertion here
          if (keys.includes(key)) {
            ret = { ...ret, [key]: value };
          }
        }
      }

      return ret;
    };

    return columnFieldProps.reduce((acc, { identifierField, dataType }) => {
      const currentPrivacyAttrs = getKeysOnField(
        ['identifierType', 'logicalType', 'securityClassification', 'nonOwner'],
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
  }

  /**
   * Computed prop over the current Id fields in the Privacy Policy
   * @type {ComputedProperty<ISchemaFieldsToPolicy>}
   */
  columnIdFieldsToCurrentPrivacyPolicy = computed(
    `{schemaFieldNamesMappedToDataTypes,${policyComplianceEntitiesKey}.[]}`,
    function(this: DatasetCompliance): ISchemaFieldsToPolicy {
      const { complianceEntities = [], modifiedTime = '0' } = get(this, 'complianceInfo') || {};
      // Truncated list of Dataset field names and data types currently returned from the column endpoint
      const columnFieldProps = getWithDefault(this, 'schemaFieldNamesMappedToDataTypes', []).map(
        ({ fieldName, dataType }) => ({
          identifierField: fieldName,
          dataType
        })
      );

      return this.mapColumnIdFieldsToCurrentPrivacyPolicy(columnFieldProps, complianceEntities, {
        policyModificationTime: modifiedTime
      });
    }
  );

  /**
   * Creates a mapping of compliance suggestions to identifierField
   * This improves performance in a subsequent merge op since this loop
   * happens only once and is cached
   * @type {ComputedProperty<ISchemaFieldsToSuggested>}
   * @memberof DatasetCompliance
   */
  identifierFieldToSuggestion = computed('complianceSuggestion', function(
    this: DatasetCompliance
  ): ISchemaFieldsToSuggested {
    const identifierFieldToSuggestion: ISchemaFieldsToSuggested = {};
    const complianceSuggestion = get(this, 'complianceSuggestion') || {
      lastModified: 0,
      suggestedFieldClassification: <any[]>[]
    };
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
  });

  /**
   * Caches a reference to the generated list of merged data between the column api and the current compliance entities list
   * @type {ComputedProperty<IComplianceChangeSet>}
   * @memberof DatasetCompliance
   */
  compliancePolicyChangeSet = computed('columnIdFieldsToCurrentPrivacyPolicy', function(
    this: DatasetCompliance
  ): Array<IComplianceChangeSet> {
    // schemaFieldNamesMappedToDataTypes is a dependency for cp columnIdFieldsToCurrentPrivacyPolicy, so no need to dep on that directly
    // TODO: move source to TS
    return mergeMappedColumnFieldsWithSuggestions(
      get(this, 'columnIdFieldsToCurrentPrivacyPolicy'),
      get(this, 'identifierFieldToSuggestion')
    );
  });

  /**
   * Returns a list of changeSet fields that meets the user selected filter criteria
   * @type {ComputedProperty<IComplianceChangeSet>}
   * @memberof DatasetCompliance
   */
  filteredChangeSet = computed('changeSetReviewCount', 'fieldReviewOption', 'compliancePolicyChangeSet', function(
    this: DatasetCompliance
  ): Array<IComplianceChangeSet> {
    const changeSet = get(this, 'compliancePolicyChangeSet');

    return get(this, 'fieldReviewOption') === 'showReview' ? changeSetFieldsRequiringReview(changeSet) : changeSet;
  });

  /**
   * Returns a count of changeSet fields that require user attention
   * @type {ComputedProperty<number>}
   * @memberof DatasetCompliance
   */
  changeSetReviewCount = computed(
    'compliancePolicyChangeSet.@each.{isDirty,suggestion,privacyPolicyExists,suggestionAuthority}',
    function(this: DatasetCompliance): number {
      return changeSetFieldsRequiringReview(get(this, 'compliancePolicyChangeSet')).length;
    }
  );

  /**
   * TODO:DSS-6719 refactor into mixin
   * Clears recently shown user messages
   * @returns {(Pick<DatasetCompliance, '_message' | '_alertType'>)}
   * @memberof DatasetCompliance
   */
  clearMessages(this: DatasetCompliance): Pick<DatasetCompliance, '_message' | '_alertType'> {
    return setProperties(this, {
      _message: '',
      _alertType: ''
    });
  }

  /**
   * Helper method to update user when an async server update to the
   * security specification is handled.
   * @template T
   * @param {Promise<T>} request the server request
   * @param {{successMessage?: string, isSaving?: boolean}} [{ successMessage = successUpdating, isSaving = false }={}]
   * @prop {successMessage} optional message for successful response
   * @prop {isSaving} optional flag indicating when the user intends to persist / save
   * @returns {Promise<void>}
   * @memberof DatasetCompliance
   */
  whenRequestCompletes<T extends { status: ApiStatus }>(
    this: DatasetCompliance,
    request: Promise<T>,
    { successMessage = successUpdating, isSaving = false }: { successMessage?: string; isSaving?: boolean } = {}
  ): Promise<void> {
    const { notify } = get(this, 'notifications');

    return Promise.resolve(request)
      .then(({ status = ApiStatus.ERROR }): void | Promise<void> => {
        return status === ApiStatus.OK
          ? notify(NotificationEvent.success, { content: successMessage })
          : Promise.reject(new Error(`Reason code for this is ${status}`));
      })
      .catch((err: string) => {
        let message = `${failedUpdating} \n ${err}`;

        if (get(this, 'isNewComplianceInfo') && !isSaving) {
          return notify(NotificationEvent.info, {
            content: 'This dataset does not have any previously saved fields with a identifying information.'
          });
        }

        notify(NotificationEvent.error, { content: message });
      });
  }

  /**
   * Sets the default classification for the given identifier field
   * Using the identifierType, determine the field's default security classification based on a values
   * supplied by complianceDataTypes endpoint
   * @param {string} identifierField the field for which the default classification should apply
   * @param {ComplianceFieldIdValue} identifierType the value of the field's identifier type
   */
  setDefaultClassification(
    this: DatasetCompliance,
    { identifierField, identifierType }: Pick<IComplianceEntity, 'identifierField' | 'identifierType'>
  ) {
    const complianceDataTypes = get(this, 'complianceDataTypes');
    const defaultSecurityClassification = getDefaultSecurityClassification(complianceDataTypes, identifierType);

    this.actions.onFieldClassificationChange.call(this, { identifierField }, { value: defaultSecurityClassification });
  }

  /**
   * Requires that the user confirm that any non-id fields are ok to be saved without a field format specified
   * @returns {Promise<boolean>}
   */
  async confirmUnformattedFields(this: DatasetCompliance): Promise<boolean> {
    type FormattedAndUnformattedEntities = {
      formatted: Array<IComplianceEntity>;
      unformatted: Array<IComplianceEntity>;
    };
    // Current list of compliance entities on policy
    const { complianceEntities = [] } = get(this, 'complianceInfo') || {};
    const formattedAndUnformattedEntities: FormattedAndUnformattedEntities = { formatted: [], unformatted: [] };
    // All candidate fields that can be on policy, excluding tracking type fields
    const changeSetEntities: Array<IComplianceEntity> = get(this, 'compliancePolicyChangeSet').map(
      ({ identifierField, identifierType = null, logicalType, nonOwner, securityClassification }) => ({
        identifierField,
        identifierType,
        logicalType,
        nonOwner,
        securityClassification
      })
    );

    // Fields that do not have a logicalType, and no identifierType or identifierType is ComplianceFieldIdValue.None
    const { formatted, unformatted }: FormattedAndUnformattedEntities = changeSetEntities.reduce(
      ({ formatted, unformatted }, field) => {
        const { identifierType, logicalType } = getProperties(field, ['identifierType', 'logicalType']);

        if (!logicalType && (ComplianceFieldIdValue.None === identifierType || !identifierType)) {
          unformatted = [...unformatted, field];
        } else {
          formatted = [...formatted, field];
        }

        return { formatted, unformatted };
      },
      formattedAndUnformattedEntities
    );

    const dialogActions = <IConfirmOptions['dialogActions']>{};
    let isConfirmed = true;
    let unformattedChangeSetEntities: Array<IComplianceEntity> = [];

    // If there are unformatted fields, require confirmation from user
    if (unformatted.length) {
      unformattedChangeSetEntities = unformatted.map(({ identifierField }) => ({
        identifierField,
        identifierType: ComplianceFieldIdValue.None,
        logicalType: null,
        securityClassification: null,
        nonOwner: false
      }));

      const confirmHandler = (function() {
        return new Promise((resolve, reject) => {
          dialogActions['didConfirm'] = () => resolve();
          dialogActions['didDismiss'] = () => reject();
        });
      })();

      // Create confirmation dialog
      get(this, 'notifications').notify(NotificationEvent.confirm, {
        header: 'Confirm fields marked as `none`',
        content: `There are ${unformatted.length} non-ID fields. `,
        dialogActions: dialogActions
      });

      try {
        await confirmHandler;
      } catch (e) {
        isConfirmed = false;
      }
    }

    isConfirmed && complianceEntities.setObjects([...formatted, ...unformattedChangeSetEntities]);

    return isConfirmed;
  }

  /**
   * Ensures the fields in the updated list of compliance entities meet the criteria
   * checked in the function. If criteria is not met, an the returned promise is settled
   * in a rejected state, otherwise fulfilled
   * @method
   * @return {any | Promise<any>}
   */
  validateFields(this: DatasetCompliance) {
    const { notify } = get(this, 'notifications');
    const { complianceEntities = [] } = get(this, 'complianceInfo') || {};
    const idTypeIdentifiers = getIdTypeDataTypes(get(this, 'complianceDataTypes'));
    const idTypeComplianceEntities = complianceEntities.filter(({ identifierType }) =>
      idTypeIdentifiers.includes(identifierType)
    );

    // Validation operations
    const idFieldsHaveValidLogicalType = idTypeComplianceEntities.every(({ logicalType }) => !!logicalType);
    const fieldIdentifiersAreUnique = isListUnique(complianceEntities.mapBy('identifierField'));
    const schemaFieldLengthGreaterThanComplianceEntities = this.isSchemaFieldLengthGreaterThanComplianceEntities();

    if (!fieldIdentifiersAreUnique) {
      notify(NotificationEvent.error, { content: complianceFieldNotUnique });
      return Promise.reject(new Error(complianceFieldNotUnique));
    }

    if (!schemaFieldLengthGreaterThanComplianceEntities) {
      notify(NotificationEvent.error, { content: complianceDataException });
      return Promise.reject(new Error(complianceFieldNotUnique));
    }

    if (!idFieldsHaveValidLogicalType) {
      return Promise.reject(notify(NotificationEvent.error, { content: missingTypes }));
    }
  }

  /**
   * Gets a reference to the current dataset classification object
   */
  getDatasetClassificationRef(this: DatasetCompliance): DatasetClassification {
    const complianceInfo = get(this, 'complianceInfo');

    if (!complianceInfo) {
      return <DatasetClassification>{};
    }

    let { datasetClassification } = complianceInfo;

    // For datasets initially without a datasetClassification, the default value is null
    if (datasetClassification === null) {
      datasetClassification = set(complianceInfo, 'datasetClassification', <DatasetClassification>{});
    }

    return datasetClassification;
  }

  /**
   * Display a modal dialog requesting that the user check affirm that the purge type is exempt
   * @return {Promise<void>}
   */
  showPurgeExemptionWarning(this: DatasetCompliance) {
    const dialogActions = <IConfirmOptions['dialogActions']>{};

    get(this, 'notifications').notify(NotificationEvent.confirm, {
      header: 'Confirm purge exemption',
      content:
        'By choosing this option you understand that either Legal or HSEC may contact you to verify the purge exemption',
      dialogActions
    });

    return new Promise((resolve, reject) => {
      dialogActions['didConfirm'] = () => resolve();
      dialogActions['didDismiss'] = () => reject();
    });
  }

  /**
   * Notifies the user to provide a missing purge policy
   * @return {Promise<never>}
   */
  needsPurgePolicyType(this: DatasetCompliance) {
    return Promise.reject(get(this, 'notifications').notify(NotificationEvent.error, { content: missingPurgePolicy }));
  }

  /**
   * Updates the currently active step in the edit sequence
   * @param {number} step
   */
  updateStep(this: DatasetCompliance, step: number) {
    set(this, 'editStepIndex', step);
  }

  actions: IDatasetComplianceActions = {
    /**
     * Sets each datasetClassification value as false
     * @returns {Promise<DatasetClassification>}
     */
    async markDatasetAsNotContainingMemberData(this: DatasetCompliance): Promise<DatasetClassification | void> {
      const dialogActions = <IConfirmOptions['dialogActions']>{};
      const confirmMarkAllHandler = new Promise((resolve, reject) => {
        dialogActions.didDismiss = () => reject();
        dialogActions.didConfirm = () => resolve();
      });
      let willMarkAllAsNo = true;

      get(this, 'notifications').notify(NotificationEvent.confirm, {
        content: 'Are you sure that any this dataset does not contain any of the listed types of member data?',
        header: 'Dataset contains no member data',
        dialogActions
      });

      try {
        await confirmMarkAllHandler;
      } catch (e) {
        willMarkAllAsNo = false;
      }

      if (willMarkAllAsNo) {
        return <DatasetClassification>setProperties(
          this.getDatasetClassificationRef(),
          datasetClassifiersKeys.reduce(
            (classification, classifier) => ({ ...classification, ...{ [classifier]: false } }),
            {}
          )
        );
      }
    },

    /**
     * Toggles the flag to show all member potential member data fields that may be contained in this dataset
     * @returns {boolean}
     */
    onShowAllDatasetMemberData(this: DatasetCompliance): boolean {
      return this.toggleProperty('showAllDatasetMemberData');
    },

    /**
     * Updates the fieldReviewOption with the user selected value
     * @param {{value: ShowAllShowReview}} { value }
     * @returns {ShowAllShowReview}
     */
    onFieldReviewChange(this: DatasetCompliance, { value }: { value: ShowAllShowReview }): ShowAllShowReview {
      return set(this, 'fieldReviewOption', value);
    },

    /**
     * Progresses 1 step backward in the edit sequence
     */
    previousStep(this: DatasetCompliance) {
      const editStepIndex = get(this, 'editStepIndex');
      const previousIndex = editStepIndex > 0 ? editStepIndex - 1 : editStepIndex;
      this.updateStep(previousIndex);
    },

    /**
     * Progresses 1 step forward in the edit sequence
     */
    nextStep(this: DatasetCompliance) {
      const { editStepIndex, editSteps } = getProperties(this, ['editStepIndex', 'editSteps']);
      const nextIndex = editStepIndex < editSteps.length - 1 ? editStepIndex + 1 : editStepIndex;
      this.updateStep(nextIndex);
    },

    /**
     * Handler for setting the dataset classification into edit mode and rendering into DOM
     * @returns {Promise<boolean>}
     */
    async didEditCompliancePolicy(this: DatasetCompliance): Promise<boolean> {
      const isConfirmed = await this.confirmUnformattedFields();

      if (isConfirmed) {
        // Ensure that the fields on the policy meet the validation criteria before proceeding
        // Otherwise exit early
        try {
          await this.validateFields();
        } catch (e) {
          // Flag this dataset's data as problematic
          if (e instanceof Error && [complianceDataException, complianceFieldNotUnique].includes(e.message)) {
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
     * Handles tasks to be processed after the wizard step to edit a datasets pii and security classification is
     * completed
     * @returns {Promise<void>}
     */
    async didEditDatasetLevelCompliancePolicy(this: DatasetCompliance): Promise<void> {
      const complianceInfo = get(this, 'complianceInfo');

      if (complianceInfo) {
        const { confidentiality, containingPersonalData } = complianceInfo;

        // defaults the containing personal data flag to false if undefined
        if (typeof containingPersonalData === 'undefined') {
          set(complianceInfo, 'containingPersonalData', false);
        }

        if (!confidentiality) {
          get(this, 'notifications').notify(NotificationEvent.error, {
            content: missingDatasetSecurityClassification
          });

          return Promise.reject(new Error(missingDatasetSecurityClassification));
        }
      }
    },

    /**
     * Handles post processing tasks after the purge policy step has been completed
     * @returns {(Promise<void | {}>)}
     */
    didEditPurgePolicy(this: DatasetCompliance): Promise<void | {}> {
      const { complianceType = null } = get(this, 'complianceInfo') || {};

      if (!complianceType) {
        return this.needsPurgePolicyType();
      }

      if (isExempt(complianceType)) {
        return this.showPurgeExemptionWarning();
      }

      return Promise.resolve();
    },

    /**
     * Augments the field props with w a suggestionAuthority indicating that the field
     * suggestion has either been accepted or ignored, and assigns the value of that change to the prop
     * @param {IComplianceChangeSet} field field for which this suggestion intent should apply
     * @param {SuggestionIntent} [intent=SuggestionIntent.ignore] user's intended action for suggestion, Defaults to `ignore`
     */
    onFieldSuggestionIntentChange(
      this: DatasetCompliance,
      field: IComplianceChangeSet,
      intent: SuggestionIntent = SuggestionIntent.ignore
    ) {
      set(field, 'suggestionAuthority', intent);
    },

    /**
     * Receives the json representation for compliance and applies each key to the policy
     * @param {string} textString string representation for the JSON file
     */
    onComplianceJsonUpload(this: DatasetCompliance, textString: string) {
      const complianceInfo = get(this, 'complianceInfo');
      const { notify } = get(this, 'notifications');
      let policy;

      if (!complianceInfo) {
        notify(NotificationEvent.error, {
          content: 'Could not find compliance current compliance policy for this dataset'
        });

        return;
      }

      try {
        policy = JSON.parse(textString);
      } catch (e) {
        notify(NotificationEvent.error, {
          content: invalidPolicyData
        });
      }

      if (isPolicyExpectedShape(policy)) {
        setProperties(complianceInfo, {
          complianceEntities: policy.complianceEntities,
          datasetClassification: policy.datasetClassification
        });

        notify(NotificationEvent.info, {
          content: successUploading
        });
      }

      notify(NotificationEvent.error, {
        content: invalidPolicyData
      });
    },

    /**
     * Handles the compliance policy download action
     */
    onComplianceDownloadJson(this: DatasetCompliance) {
      const currentPolicy = get(this, 'complianceInfo');

      if (!currentPolicy) {
        return get(this, 'notifications').notify(NotificationEvent.error, {
          content: 'Could not find the current policy to download'
        });
      }

      const policy = Object.assign({}, getProperties(currentPolicy, ['datasetClassification', 'complianceEntities']));
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
    onFieldIdentifierTypeChange(
      this: DatasetCompliance,
      { identifierField }: IComplianceChangeSet,
      { value: identifierType }: { value: ComplianceFieldIdValue }
    ) {
      const complianceEntitiesChangeSet = get(this, 'compliancePolicyChangeSet');
      // A reference to the current field in the compliance list, it should exist even for empty complianceEntities
      // since this is a reference created in the working copy: compliancePolicyChangeSet
      const changeSetComplianceField = complianceEntitiesChangeSet.findBy('identifierField', identifierField);

      // Reset field attributes on change to field in change set
      if (changeSetComplianceField) {
        setProperties(changeSetComplianceField, <IComplianceChangeSet>{
          identifierType,
          logicalType: null,
          nonOwner: false,
          isDirty: true
        });
      }

      // Set the defaultClassification for the identifierField,
      this.setDefaultClassification({ identifierField, identifierType });
    },

    /**
     * Updates the logical type for the given identifierField
     * @param {IComplianceChangeSet} field
     * @param {IComplianceChangeSet.logicalType} logicalType
     */
    onFieldLogicalTypeChange(
      this: DatasetCompliance,
      field: IComplianceChangeSet,
      logicalType: IComplianceChangeSet['logicalType']
    ) {
      setProperties(field, <IComplianceChangeSet>{ logicalType, isDirty: true });
    },

    /**
     * Updates the field security classification
     * @param {IComplianceChangeSet} { identifierField } the identifier field to update the classification for
     * @param {{value: IComplianceChangeSet.classification}} { value: classification = null }
     */
    onFieldClassificationChange(
      this: DatasetCompliance,
      { identifierField }: IComplianceChangeSet,
      { value: securityClassification = null }: { value: IComplianceChangeSet['securityClassification'] }
    ) {
      const currentFieldInComplianceList = get(this, 'compliancePolicyChangeSet').findBy(
        'identifierField',
        identifierField
      );
      // TODO:DSS-6719 refactor into mixin
      this.clearMessages();

      // Apply the updated classification value to the current instance of the field in working copy
      if (currentFieldInComplianceList) {
        setProperties(currentFieldInComplianceList, <IComplianceChangeSet>{
          securityClassification,
          isDirty: true
        });
      }
    },

    /**
     * Updates the field non owner flag
     * @param {IComplianceChangeSet} { identifierField }
     * @param {IComplianceChangeSet.nonOwner} nonOwner
     */
    onFieldOwnerChange(
      this: DatasetCompliance,
      { identifierField }: IComplianceChangeSet,
      nonOwner: IComplianceChangeSet['nonOwner']
    ) {
      const currentFieldInComplianceList = get(this, 'compliancePolicyChangeSet').findBy(
        'identifierField',
        identifierField
      );
      if (currentFieldInComplianceList) {
        setProperties(currentFieldInComplianceList, <IComplianceChangeSet>{ nonOwner, isDirty: true });
      }
    },

    /**
     * Updates the source object representing the current datasetClassification map
     * @param {keyof typeof DatasetClassifiers} classifier the property on the datasetClassification to update
     * @param {boolean} value
     * @returns
     */
    onChangeDatasetClassification<K extends keyof typeof DatasetClassifiers>(
      this: DatasetCompliance,
      classifier: K,
      value: DatasetClassification[K]
    ) {
      return set(this.getDatasetClassificationRef(), classifier, value);
    },

    /**
     * Updates the complianceType on the compliance policy
     * @param {PurgePolicy} purgePolicy
     * @returns {IComplianceInfo.complianceType}
     */
    onDatasetPurgePolicyChange(
      this: DatasetCompliance,
      purgePolicy: PurgePolicy
    ): IComplianceInfo['complianceType'] | null {
      const complianceInfo = get(this, 'complianceInfo');

      if (!complianceInfo) {
        return null;
      }
      // directly set the complianceType to the updated value
      return set(complianceInfo, 'complianceType', purgePolicy);
    },

    /**
     * Updates the policy flag indicating that this dataset contains personal data
     * @param {boolean} containsPersonalData
     * @returns {boolean}
     */
    onDatasetLevelPolicyChange(this: DatasetCompliance, containsPersonalData: boolean): boolean | null {
      const complianceInfo = get(this, 'complianceInfo');
      // directly mutate the attribute on the complianceInfo object
      return complianceInfo ? set(complianceInfo, 'containingPersonalData', containsPersonalData) : null;
    },

    /**
     * Updates the confidentiality flag on the dataset compliance
     * @param {IComplianceInfo.confidentiality} [securityClassification=null]
     * @returns {IComplianceInfo.confidentiality}
     */
    onDatasetSecurityClassificationChange(
      this: DatasetCompliance,
      securityClassification: IComplianceInfo['confidentiality'] = null
    ): IComplianceInfo['confidentiality'] {
      const complianceInfo = get(this, 'complianceInfo');

      return complianceInfo ? set(complianceInfo, 'confidentiality', securityClassification) : null;
    },

    /**
     * If all validity checks are passed, invoke onSave action on controller
     */
    async saveCompliance(this: DatasetCompliance): Promise<void> {
      const setSaveFlag = (flag = false) => set(this, 'isSaving', flag);

      try {
        const isSaving = true;
        const onSave = get(this, 'onSave');
        setSaveFlag(isSaving);

        await this.whenRequestCompletes(onSave(), { isSaving });
        return this.updateStep(-1);
      } finally {
        setSaveFlag();
      }
    },

    // Rolls back changes made to the compliance spec to current
    // server state
    resetCompliance(this: DatasetCompliance) {
      const options = {
        successMessage: 'Field classification has been reset to the previously saved state.'
      };
      this.whenRequestCompletes(get(this, 'onReset')(), options);
    }
  };
}
