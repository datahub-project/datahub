import Component from '@ember/component';
import { computed, set, get, setProperties, getProperties, getWithDefault } from '@ember/object';
import ComputedProperty, { not, or, alias } from '@ember/object/computed';
import { run, schedule, next } from '@ember/runloop';
import { inject } from '@ember/service';
import { classify } from '@ember/string';
import { assert } from '@ember/debug';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { IDataPlatform } from 'wherehows-web/typings/api/list/platforms';
import { readPlatforms } from 'wherehows-web/utils/api/list/platforms';
import { task, waitForProperty, TaskInstance } from 'ember-concurrency';
import {
  getSecurityClassificationDropDownOptions,
  DatasetClassifiers,
  getFieldIdentifierOptions,
  getDefaultSecurityClassification,
  compliancePolicyStrings,
  getComplianceSteps,
  isExempt,
  ComplianceFieldIdValue,
  IDatasetClassificationOption,
  DatasetClassification,
  SuggestionIntent,
  PurgePolicy,
  getSupportedPurgePolicies,
  mergeComplianceEntitiesWithSuggestions,
  tagsRequiringReview,
  isTagIdType,
  idTypeTagsHaveLogicalType,
  changeSetReviewableAttributeTriggers,
  asyncMapSchemaColumnPropsToCurrentPrivacyPolicy,
  foldComplianceChangeSets,
  sortFoldedChangeSetTuples,
  tagsWithoutIdentifierType,
  singleTagsInChangeSet,
  tagsForIdentifierField,
  overrideTagReadonly,
  editableTags,
  lowQualitySuggestionConfidenceThreshold
} from 'wherehows-web/constants';
import { getTagsSuggestions } from 'wherehows-web/utils/datasets/compliance-suggestions';
import { arrayMap, compact, isListUnique, iterateArrayAsync } from 'wherehows-web/utils/array';
import { noop } from 'wherehows-web/utils/helpers/functions';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';
import { IDatasetColumn } from 'wherehows-web/typings/api/datasets/columns';
import {
  IComplianceInfo,
  IComplianceEntity,
  ISuggestedFieldClassification,
  IComplianceSuggestion
} from 'wherehows-web/typings/api/datasets/compliance';
import {
  IComplianceChangeSet,
  IComplianceEntityWithMetadata,
  IComplianceFieldIdentifierOption,
  IDatasetComplianceActions,
  IdentifierFieldWithFieldChangeSetTuple,
  IDropDownOption,
  ISchemaFieldsToPolicy,
  ISchemaFieldsToSuggested,
  ISecurityClassificationOption,
  ShowAllShowReview
} from 'wherehows-web/typings/app/dataset-compliance';
import { uniqBy } from 'lodash';
import { IColumnFieldProps } from 'wherehows-web/typings/app/dataset-columns';
import { NonIdLogicalType } from 'wherehows-web/constants/datasets/compliance';
import { pick } from 'lodash';
import { trackableEvent, TrackableEventCategory } from 'wherehows-web/constants/analytics/event-tracking';
import { notificationDialogActionFactory } from 'wherehows-web/utils/notifications/notifications';
import validateMetadataObject, {
  complianceEntitiesTaxonomy
} from 'wherehows-web/utils/datasets/compliance/metadata-schema';
import { typeOf } from '@ember/utils';

const {
  complianceDataException,
  complianceFieldNotUnique,
  missingTypes,
  missingPurgePolicy,
  missingDatasetSecurityClassification
} = compliancePolicyStrings;

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
 * The initial state of the compliance step for a zero based array
 * @type {number}
 */
const initialStepIndex = -1;

export default class DatasetCompliance extends Component {
  isNewComplianceInfo: boolean;
  datasetName: string;
  sortColumnWithName: string;
  filterBy: string;
  sortDirection: string;
  searchTerm: string;
  _hasBadData: boolean;
  platform: IDatasetView['platform'];
  isCompliancePolicyAvailable: boolean = false;
  showAllDatasetMemberData: boolean;
  complianceInfo: undefined | IComplianceInfo;

  /**
   * Lists the compliance entities that are entered via the advanced editing interface
   * @type {Pick<IComplianceInfo, 'complianceEntities'>}
   * @memberof DatasetCompliance
   */
  manuallyEnteredComplianceEntities: Pick<IComplianceInfo, 'complianceEntities'>;

  /**
   * Flag enabling or disabling the manual apply button
   * @type {boolean}
   * @memberof DatasetCompliance
   */
  isManualApplyDisabled: boolean = false;

  /**
   * String representation of a parse error that may have occurred when validating manually entered compliance entities
   * @type {string}
   * @memberof DatasetCompliance
   */
  manualParseError: string = '';

  /**
   * Flag indicating the current compliance policy edit-view mode
   * @type {boolean}
   * @memberof DatasetCompliance
   */
  showGuidedComplianceEditMode: boolean = true;

  /**
   * Confidence percentage number used to filter high quality suggestions versus lower quality
   * @type {number}
   * @memberof DatasetCompliance
   */
  suggestionConfidenceThreshold: number;

  /**
   * Formatted JSON string representing the compliance entities for this dataset
   * @type {ComputedProperty<string>}
   */
  jsonComplianceEntities: ComputedProperty<string> = computed('columnIdFieldsToCurrentPrivacyPolicy', function(
    this: DatasetCompliance
  ): string {
    const entityAttrs = ['identifierField', 'identifierType', 'logicalType', 'nonOwner', 'valuePattern', 'readonly'];
    const entityMap: ISchemaFieldsToPolicy = get(this, 'columnIdFieldsToCurrentPrivacyPolicy');
    const entitiesWithModifiableKeys = arrayMap((tag: IComplianceEntityWithMetadata) => pick(tag, entityAttrs))(
      (<Array<IComplianceEntityWithMetadata>>[]).concat(...Object.values(entityMap))
    );

    return JSON.stringify(entitiesWithModifiableKeys, null, '\t');
  });

  /**
   * Convenience computed property flag indicates if current edit step is the first step in  the wizard flow
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetCompliance
   */
  isInitialEditStep = computed('editStep', 'editSteps.0.name', function(this: DatasetCompliance): boolean {
    const { editStep, editSteps } = getProperties(this, ['editStep', 'editSteps']);
    const [initialStep] = editSteps;
    return editStep.name === initialStep.name;
  });

  /**
   * Indicates if the first step does not need further user review to advance
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetCompliance
   */
  initialStepNeedsReview = computed('isInitialEditStep', 'changeSetReviewWithoutSuggestionCheck', function(
    this: DatasetCompliance
  ): boolean {
    const { isInitialEditStep, changeSetReviewWithoutSuggestionCheck } = getProperties(this, [
      'isInitialEditStep',
      'changeSetReviewWithoutSuggestionCheck'
    ]);
    const { length } = editableTags(changeSetReviewWithoutSuggestionCheck);

    return isInitialEditStep && length > 0;
  });

  /**
   * Flag indicating if the Guided vs Advanced mode should be shown for the initial edit step
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetCompliance
   */
  showAdvancedEditApplyStep = computed('isInitialEditStep', 'showGuidedComplianceEditMode', function(
    this: DatasetCompliance
  ): boolean {
    const { isInitialEditStep, showGuidedComplianceEditMode } = getProperties(this, [
      'isInitialEditStep',
      'showGuidedComplianceEditMode'
    ]);
    return isInitialEditStep && !showGuidedComplianceEditMode;
  });

  /**
   * Flag indicating the readonly confirmation dialog should not be shown again for this compliance form
   * @type {boolean}
   */
  doNotShowReadonlyConfirmation: boolean = false;

  /**
   * References the ComplianceFieldIdValue enum
   * @type {ComplianceFieldIdValue}
   */
  ComplianceFieldIdValue = ComplianceFieldIdValue;

  /**
   * Suggested values for compliance types e.g. identifier type and/or logical type
   * @type {IComplianceSuggestion | void}
   */
  complianceSuggestion: IComplianceSuggestion | void;

  schemaFieldNamesMappedToDataTypes: Array<Pick<IDatasetColumn, 'dataType' | 'fieldName'>>;
  onReset: <T>() => Promise<T>;
  onSave: <T>() => Promise<T>;

  /**
   * External action to handle manual compliance entity metadata entry
   */
  onComplianceJsonUpdate: (jsonString: string) => Promise<void>;

  notifyOnChangeSetSuggestions: (hasSuggestions: boolean) => void;
  notifyOnChangeSetRequiresReview: (hasChangeSetDrift: boolean) => void;

  classNames = ['compliance-container'];

  classNameBindings = ['isEditing:compliance-container--edit-mode'];

  /**
   * Reference to the application notifications Service
   * @type {ComputedProperty<Notifications>}
   */
  notifications: ComputedProperty<Notifications> = inject();

  /**
   * Flag indicating that the field names in each compliance row is truncated or rendered in full
   * @type {boolean}
   */
  isShowingFullFieldNames = true;

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
  editStepIndex: number;

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
  isEditing = computed('editStepIndex', 'complianceInfo.fromUpstream', function(this: DatasetCompliance): boolean {
    // initialStepIndex is less than the currently set step index
    return get(this, 'editStepIndex') > initialStepIndex;
  });

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

  /**
   * Computed prop over the current Id fields in the Privacy Policy
   * @type {ISchemaFieldsToPolicy}
   */
  columnIdFieldsToCurrentPrivacyPolicy: ISchemaFieldsToPolicy = {};

  /**
   * Enum of categories that can be tracked for this component
   * @type {TrackableEventCategory}
   */
  trackableCategory = TrackableEventCategory;

  /**
   * Map of events that can be tracked
   * @type {ITrackableEventCategoryEvent}
   */
  trackableEvent = trackableEvent;

  constructor() {
    super(...arguments);

    //sets default values for class fields
    this.editStepIndex = initialStepIndex;
    this.sortColumnWithName || set(this, 'sortColumnWithName', 'identifierField');
    this.filterBy || set(this, 'filterBy', '0'); // first element in field type is identifierField
    this.sortDirection || set(this, 'sortDirection', 'asc');
    this.searchTerm || set(this, 'searchTerm', '');
    this.schemaFieldNamesMappedToDataTypes || (this.schemaFieldNamesMappedToDataTypes = []);
    this.complianceDataTypes || (this.complianceDataTypes = []);
    typeOf(this.suggestionConfidenceThreshold) === 'number' ||
      set(this, 'suggestionConfidenceThreshold', lowQualitySuggestionConfidenceThreshold);
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
      .map((key: string): { name: string } => steps[+key]);
  });

  /**
   * Reads the complianceDataTypes property and transforms into a list of drop down options for the field
   * identifier type
   * @type {ComputedProperty<Array<IComplianceFieldIdentifierOption  | IDropDownOption<null | 'NONE'>>>}
   */
  complianceFieldIdDropdownOptions = computed('complianceDataTypes', function(
    this: DatasetCompliance
  ): Array<IComplianceFieldIdentifierOption | IDropDownOption<null | ComplianceFieldIdValue.None>> {
    // object with interface IComplianceDataType and an index number indicative of position
    type IndexedComplianceDataType = IComplianceDataType & { index: number };

    const noneDropDownOption: IDropDownOption<ComplianceFieldIdValue.None> = {
      value: ComplianceFieldIdValue.None,
      label: 'None'
    };
    // Creates a list of IComplianceDataType each with an index. The intent here is to perform a stable sort on
    // the items in the list, Array#sort is not stable, so for items that equal on the primary comparator
    // break the tie based on position in original list
    const indexedDataTypes: Array<IndexedComplianceDataType> = (get(this, 'complianceDataTypes') || []).map(
      (type, index): IndexedComplianceDataType => ({
        ...type,
        index
      })
    );

    /**
     * Compares each compliance data type, ensure that positional order is maintained
     * @param {IComplianceDataType} a the compliance type to compare
     * @param {IComplianceDataType} b the other
     * @returns {number} 0, 1, -1 indicating sort order
     */
    const dataTypeComparator = (a: IndexedComplianceDataType, b: IndexedComplianceDataType): number => {
      const { idType: aIdType, index: aIndex } = a;
      const { idType: bIdType, index: bIndex } = b;
      // Convert boolean values to number type
      const typeCompare = Number(aIdType) - Number(bIdType);

      // True types first, hence negation
      // If types are same, then sort on original position i.e stable sort
      return typeCompare ? -typeCompare : aIndex - bIndex;
    };

    /**
     * Inserts a divider in the list of compliance field identifier options
     * @param {Array<IComplianceFieldIdentifierOption>} types
     * @returns {Array<IComplianceFieldIdentifierOption>}
     */
    const insertDividers = (
      types: Array<IComplianceFieldIdentifierOption>
    ): Array<IComplianceFieldIdentifierOption> => {
      const isId = ({ isId }: IComplianceFieldIdentifierOption): boolean => isId;
      const ids = types.filter(isId);
      const nonIds = types.filter((type): boolean => !isId(type));
      //divider to indicate section for ids
      const idsDivider = { value: '', label: 'First Party IDs', isDisabled: true };
      // divider to indicate section for non ids
      const nonIdsDivider = { value: '', label: 'Non First Party IDs', isDisabled: true };

      return [
        <IComplianceFieldIdentifierOption>idsDivider,
        ...ids,
        <IComplianceFieldIdentifierOption>nonIdsDivider,
        ...nonIds
      ];
    };

    return [
      noneDropDownOption,
      ...insertDividers(getFieldIdentifierOptions(indexedDataTypes.sort(dataTypeComparator)))
    ];
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
          run((): void => {
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
  editStep: { name: string } = { name: '' };

  /**
   * A list of ui values and labels for review filter drop-down
   * @type {Array<{value: string, label:string}>}
   * @memberof DatasetCompliance
   */
  fieldReviewOptions: Array<{ value: DatasetCompliance['fieldReviewOption']; label: string }> = [
    { value: 'showAll', label: 'Showing all fields' },
    { value: 'showReview', label: 'Showing only fields to review' }
  ];

  didReceiveAttrs(): void {
    // Perform validation step on the received component attributes
    this.validateAttrs();

    // Set the current step to first edit step if compliance policy is new / doesn't exist
    if (get(this, 'isNewComplianceInfo')) {
      this.updateStep(0);
    }
  }

  didInsertElement(): void {
    get(this, 'complianceAvailabilityTask').perform();
    get(this, 'columnFieldsToCompliancePolicyTask').perform();
    get(this, 'foldChangeSetTask').perform();
  }

  didUpdateAttrs(): void {
    get(this, 'columnFieldsToCompliancePolicyTask').perform();
    get(this, 'foldChangeSetTask').perform();
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
   * Checks that dataset content types have a boolean value
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

    // `datasetClassification` is nullable hence default
    return Object.values(datasetClassification || {}).some(hasMemberData => !hasMemberData);
  });

  /**
   * Determines if all types of data fields should be shown in the table i.e. show only fields contained in
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
  isSchemaFieldLengthGreaterThanUniqComplianceEntities(this: DatasetCompliance): boolean {
    const complianceInfo = get(this, 'complianceInfo');
    if (complianceInfo) {
      const { length: columnFieldsLength } = getWithDefault(this, 'schemaFieldNamesMappedToDataTypes', []);
      const { length: complianceListLength } = uniqBy(
        get(complianceInfo, 'complianceEntities') || [],
        'identifierField'
      );

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
            value: datasetClassification ? datasetClassification[classifier] : void 0, // undefined !== false, tri-state
            label: DatasetClassifiers[classifier]
          }
        ];
      }, []);
    }

    return [];
  });

  /**
   * Task to retrieve column fields async and set values on Component
   * @type {Task<Promise<any>, () => TaskInstance<Promise<any>>>}
   * @memberof DatasetCompliance
   */
  columnFieldsToCompliancePolicyTask = task(function*(this: DatasetCompliance): IterableIterator<any> {
    // Truncated list of Dataset field names and data types currently returned from the column endpoint
    const schemaFieldNamesMappedToDataTypes: DatasetCompliance['schemaFieldNamesMappedToDataTypes'] = yield waitForProperty(
      this,
      'schemaFieldNamesMappedToDataTypes',
      ({ length }) => !!length
    );

    const { complianceEntities = [], modifiedTime }: Pick<IComplianceInfo, 'complianceEntities' | 'modifiedTime'> = get(
      this,
      'complianceInfo'
    )!;
    const renameFieldNameAttr = ({
      fieldName,
      dataType
    }: Pick<IDatasetColumn, 'dataType' | 'fieldName'>): {
      identifierField: IDatasetColumn['fieldName'];
      dataType: IDatasetColumn['dataType'];
    } => ({
      identifierField: fieldName,
      dataType
    });
    const columnProps: Array<IColumnFieldProps> = yield iterateArrayAsync(arrayMap(renameFieldNameAttr))(
      schemaFieldNamesMappedToDataTypes
    );

    const columnIdFieldsToCurrentPrivacyPolicy: ISchemaFieldsToPolicy = yield asyncMapSchemaColumnPropsToCurrentPrivacyPolicy(
      {
        columnProps,
        complianceEntities,
        policyModificationTime: modifiedTime
      }
    );

    set(this, 'columnIdFieldsToCurrentPrivacyPolicy', columnIdFieldsToCurrentPrivacyPolicy);
  }).enqueue();

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
    const fieldSuggestions: ISchemaFieldsToSuggested = {};
    const complianceSuggestion = get(this, 'complianceSuggestion') || {
      lastModified: 0,
      suggestedFieldClassification: <Array<ISuggestedFieldClassification>>[]
    };
    const { lastModified: suggestionsModificationTime, suggestedFieldClassification = [] } = complianceSuggestion;

    // If the compliance suggestions array contains suggestions the create reduced lookup map,
    // otherwise, ignore
    if (suggestedFieldClassification.length) {
      return suggestedFieldClassification.reduce(
        (
          fieldSuggestions: ISchemaFieldsToSuggested,
          { suggestion: { identifierField, identifierType, logicalType, securityClassification }, confidenceLevel, uid }
        ) => ({
          ...fieldSuggestions,
          [identifierField]: {
            identifierType,
            logicalType,
            securityClassification,
            confidenceLevel,
            uid,
            suggestionsModificationTime
          }
        }),
        fieldSuggestions
      );
    }

    return fieldSuggestions;
  });

  /**
   * Caches a reference to the generated list of merged data between the column api and the current compliance entities list
   * @type {ComputedProperty<IComplianceChangeSet>}
   * @memberof DatasetCompliance
   */
  compliancePolicyChangeSet = computed(
    'columnIdFieldsToCurrentPrivacyPolicy',
    'complianceDataTypes',
    'identifierFieldToSuggestion',
    'suggestionConfidenceThreshold',
    function(this: DatasetCompliance): Array<IComplianceChangeSet> {
      // schemaFieldNamesMappedToDataTypes is a dependency for CP columnIdFieldsToCurrentPrivacyPolicy, so no need to dep on that directly
      const changeSet = mergeComplianceEntitiesWithSuggestions(
        get(this, 'columnIdFieldsToCurrentPrivacyPolicy'),
        get(this, 'identifierFieldToSuggestion')
      );
      const suggestionThreshold = get(this, 'suggestionConfidenceThreshold');

      // pass current changeSet state to parent handlers
      run(() => next(this, 'notifyHandlerOfSuggestions', suggestionThreshold, changeSet));
      run(() =>
        next(
          this,
          'notifyHandlerOfFieldsRequiringReview',
          suggestionThreshold,
          get(this, 'complianceDataTypes'),
          changeSet
        )
      );

      return changeSet;
    }
  );

  /**
   * Returns a list of changeSet fields that meets the user selected filter criteria
   * @type {ComputedProperty<IComplianceChangeSet>}
   * @memberof DatasetCompliance
   */
  filteredChangeSet = computed(
    'changeSetReviewCount',
    'fieldReviewOption',
    'compliancePolicyChangeSet',
    'complianceDataTypes',
    'suggestionConfidenceThreshold',
    function(this: DatasetCompliance): Array<IComplianceChangeSet> {
      const {
        compliancePolicyChangeSet: changeSet,
        complianceDataTypes,
        suggestionConfidenceThreshold
      } = getProperties(this, ['compliancePolicyChangeSet', 'complianceDataTypes', 'suggestionConfidenceThreshold']);

      return get(this, 'fieldReviewOption') === 'showReview'
        ? tagsRequiringReview(complianceDataTypes, { checkSuggestions: true, suggestionConfidenceThreshold })(changeSet)
        : changeSet;
    }
  );

  /**
   * Filters out the compliance tags requiring review excluding tags that require review,
   * due to a suggestion mismatch with the current tag identifierType
   * This drives the initialStep review check and fulfills the use-case,
   * where the user can proceed with the compliance update, without
   * being required to resolve a suggestion mismatch
   * @type {ComputedProperty<Array<IComplianceChangeSet>>}
   * @memberof DatasetCompliance
   */
  changeSetReviewWithoutSuggestionCheck = computed('changeSetReview', function(
    this: DatasetCompliance
  ): Array<IComplianceChangeSet> {
    return tagsRequiringReview(get(this, 'complianceDataTypes'), {
      checkSuggestions: false,
      suggestionConfidenceThreshold: 0 // irrelevant value set to 0 since checkSuggestions flag is false above
    })(get(this, 'changeSetReview'));
  });

  /**
   * The changeSet tags that require user attention
   * @type {ComputedProperty<Array<IComplianceChangeSet>>}
   * @memberof DatasetCompliance
   */
  changeSetReview = computed(
    `compliancePolicyChangeSet.@each.{${changeSetReviewableAttributeTriggers}}`,
    'complianceDataTypes',
    'suggestionConfidenceThreshold',
    function(this: DatasetCompliance): Array<IComplianceChangeSet> {
      const { suggestionConfidenceThreshold, compliancePolicyChangeSet } = getProperties(this, [
        'suggestionConfidenceThreshold',
        'compliancePolicyChangeSet'
      ]);

      return tagsRequiringReview(get(this, 'complianceDataTypes'), {
        checkSuggestions: true,
        suggestionConfidenceThreshold
      })(compliancePolicyChangeSet);
    }
  );

  /**
   * Returns a count of changeSet tags that require user attention
   * @type {ComputedProperty<number>}
   * @memberof DatasetCompliance
   */
  changeSetReviewCount = alias('changeSetReview.length');

  /**
   * Reduces the current filtered changeSet to a list of IdentifierFieldWithFieldChangeSetTuple
   * @type {Array<IdentifierFieldWithFieldChangeSetTuple>}
   * @memberof DatasetCompliance
   */
  foldedChangeSet: Array<IdentifierFieldWithFieldChangeSetTuple>;

  /**
   * Task to retrieve platform policies and set supported policies for the current platform
   * @type {Task<Promise<any>, () => TaskInstance<Promise<any>>>}
   * @memberof DatasetCompliance
   */
  foldChangeSetTask = task(function*(this: DatasetCompliance): IterableIterator<any> {
    //@ts-ignore dot notation for property access
    yield waitForProperty(this, 'columnFieldsToCompliancePolicyTask.isIdle');
    const filteredChangeSet = get(this, 'filteredChangeSet');
    const foldedChangeSet: Array<IdentifierFieldWithFieldChangeSetTuple> = yield foldComplianceChangeSets(
      filteredChangeSet
    );

    set(this, 'foldedChangeSet', sortFoldedChangeSetTuples(foldedChangeSet));
  }).enqueue();

  /**
   * Lists the IComplianceChangeSet / tags without an identifierType value
   * @type {ComputedProperty<Array<IComplianceChangeSet>>}
   * @memberof DatasetCompliance
   */
  unspecifiedTags = computed(`compliancePolicyChangeSet.@each.{${changeSetReviewableAttributeTriggers}}`, function(
    this: DatasetCompliance
  ): Array<IComplianceChangeSet> {
    const tags = get(this, 'compliancePolicyChangeSet');
    const singleTags = singleTagsInChangeSet(tags, tagsForIdentifierField);

    return tagsWithoutIdentifierType(singleTags);
  });

  /**
   * Sets the identifierType attribute on IComplianceChangeSetFields without an identifierType to ComplianceFieldIdValue.None
   * @returns {Promise<Array<IComplianceChangeSet>>}
   */
  setUnspecifiedTagsAsNoneTask = task(function*(
    this: DatasetCompliance
  ): IterableIterator<Promise<Array<ComplianceFieldIdValue | NonIdLogicalType>>> {
    const unspecifiedTags = get(this, 'unspecifiedTags');
    const setTagIdentifier = (value: ComplianceFieldIdValue | NonIdLogicalType) => (tag: IComplianceChangeSet) =>
      set(tag, 'identifierType', value);

    yield iterateArrayAsync(arrayMap(setTagIdentifier(ComplianceFieldIdValue.None)))(unspecifiedTags);
  }).drop();

  /**
   * Invokes external action with flag indicating that at least 1 suggestion exists for a field in the changeSet
   * @param {number} suggestionConfidenceThreshold confidence threshold for filtering out higher quality suggestions
   * @param {Array<IComplianceChangeSet>} changeSet
   */
  notifyHandlerOfSuggestions = (
    suggestionConfidenceThreshold: number,
    changeSet: Array<IComplianceChangeSet>
  ): void => {
    const hasChangeSetSuggestions = !!compact(getTagsSuggestions({ suggestionConfidenceThreshold })(changeSet)).length;
    this.notifyOnChangeSetSuggestions(hasChangeSetSuggestions);
  };

  /**
   * Invokes external action with flag indicating that a field in the tags requires user review
   * @param {number} suggestionConfidenceThreshold confidence threshold for filtering out higher quality suggestions
   * @param {Array<IComplianceDataType>} complianceDataTypes
   * @param {Array<IComplianceChangeSet>} tags
   */
  notifyHandlerOfFieldsRequiringReview = (
    suggestionConfidenceThreshold: number,
    complianceDataTypes: Array<IComplianceDataType>,
    tags: Array<IComplianceChangeSet>
  ): void => {
    // adding assertions for run-loop callback invocation, because static type checks are bypassed
    assert('expected complianceDataTypes to be of type `array`', Array.isArray(complianceDataTypes));
    assert('expected tags to be of type `array`', Array.isArray(tags));

    const hasChangeSetDrift = !!tagsRequiringReview(complianceDataTypes, {
      checkSuggestions: true,
      suggestionConfidenceThreshold
    })(tags).length;

    this.notifyOnChangeSetRequiresReview(hasChangeSetDrift);
  };

  /**
   * Sets the default classification for the given identifier field's tag
   * Using the identifierType, determine the tag's default security classification based on a values
   * supplied by complianceDataTypes endpoint
   * @param {string} identifierField the field for which the default classification should apply
   * @param {ComplianceFieldIdValue} identifierType the value of the field's identifier type
   */
  setDefaultClassification(
    this: DatasetCompliance,
    { identifierField, identifierType }: Pick<IComplianceEntity, 'identifierField' | 'identifierType'>
  ): void {
    const complianceDataTypes = get(this, 'complianceDataTypes');
    const defaultSecurityClassification = getDefaultSecurityClassification(complianceDataTypes, identifierType);

    this.actions.tagClassificationChanged.call(this, { identifierField }, { value: defaultSecurityClassification });
  }

  /**
   * Maps attributes from the working copy to the compliance entities to be persisted remotely
   * @returns {Promise<Array<IComplianceEntity>>}
   */
  async applyWorkingCopy(this: DatasetCompliance): Promise<Array<IComplianceEntity>> {
    // Current list of compliance entities on policy
    const { complianceInfo, compliancePolicyChangeSet: workingCopy } = getProperties(this, [
      'complianceInfo',
      'compliancePolicyChangeSet'
    ]);
    const { complianceEntities } = complianceInfo!;
    // All changeSet attrs that can be on policy, excluding changeSet metadata
    const entityAttrs = [
      'identifierField',
      'identifierType',
      'logicalType',
      'nonOwner',
      'securityClassification',
      'readonly',
      'valuePattern'
    ];
    const updatingComplianceEntities = arrayMap(
      (tag: IComplianceChangeSet): IComplianceEntity => <IComplianceEntity>pick(tag, entityAttrs)
    )(workingCopy);

    return complianceEntities.setObjects(updatingComplianceEntities);
  }

  /**
   * Ensures the fields in the updated list of compliance entities meet the criteria
   * checked in the function. If criteria is not met, an the returned promise is settled
   * in a rejected state, otherwise fulfilled
   * @method
   * @return {Promise<never | void>}
   */
  async validateFields(this: DatasetCompliance): Promise<never | void> {
    const { notify } = get(this, 'notifications');
    const { complianceEntities = [] } = get(this, 'complianceInfo') || {};
    const idTypeComplianceEntities = complianceEntities.filter(isTagIdType(get(this, 'complianceDataTypes')));

    // Validation operations
    const idFieldsHaveValidLogicalType: boolean = idTypeTagsHaveLogicalType(idTypeComplianceEntities);
    const isSchemaFieldLengthGreaterThanUniqComplianceEntities: boolean = this.isSchemaFieldLengthGreaterThanUniqComplianceEntities();

    if (!isSchemaFieldLengthGreaterThanUniqComplianceEntities) {
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
  showPurgeExemptionWarning(this: DatasetCompliance): Promise<void> {
    const { dialogActions, dismissedOrConfirmed } = notificationDialogActionFactory();

    get(this, 'notifications').notify(NotificationEvent.confirm, {
      header: 'Confirm purge exemption',
      content:
        'By choosing this option you understand that either Legal or HSEC may contact you to verify the purge exemption',
      dialogActions
    });

    return dismissedOrConfirmed;
  }

  /**
   * Notifies the user to provide a missing purge policy
   * @return {Promise<never>}
   */
  needsPurgePolicyType(this: DatasetCompliance): Promise<never> {
    return Promise.reject(get(this, 'notifications').notify(NotificationEvent.error, { content: missingPurgePolicy }));
  }

  /**
   * Updates the currently active step in the edit sequence
   * @param {number} step
   */
  updateStep(this: DatasetCompliance, step: number): void {
    set(this, 'editStepIndex', step);
    get(this, 'updateEditStepTask').perform();
  }

  actions: IDatasetComplianceActions = {
    /**
     * Toggle the visibility of the guided compliance edit view vs the advanced edit view modes
     * @param {boolean} toggle flag ,if true, show guided edit mode, otherwise, advanced
     */
    onShowGuidedEditMode(this: DatasetCompliance, toggle: boolean): void {
      const isShowingGuidedEditMode = set(this, 'showGuidedComplianceEditMode', toggle);

      if (!isShowingGuidedEditMode) {
        this.actions.onManualComplianceUpdate.call(this, get(this, 'jsonComplianceEntities'));
      }
    },

    /**
     * Handles updating the list of compliance entities when a user manually enters values
     * for the compliance entity metadata
     * @param {string} updatedEntities json string of entities
     */
    onManualComplianceUpdate(this: DatasetCompliance, updatedEntities: string): void {
      try {
        // check if the string is parseable as a JSON object
        const entities = JSON.parse(updatedEntities);
        const metadataObject = {
          complianceEntities: entities
        };
        const isValid = validateMetadataObject(metadataObject, complianceEntitiesTaxonomy);

        setProperties(this, { isManualApplyDisabled: !isValid, manualParseError: '' });

        if (isValid) {
          set(this, 'manuallyEnteredComplianceEntities', metadataObject);
        }
      } catch (e) {
        setProperties(this, { isManualApplyDisabled: true, manualParseError: e.message });
      }
    },

    /**
     * Handler to apply manually entered compliance entities to the actual list of
     * compliance metadata entities to be saved
     */
    async onApplyComplianceJson(this: DatasetCompliance) {
      try {
        await get(this, 'onComplianceJsonUpdate')(JSON.stringify(get(this, 'manuallyEnteredComplianceEntities')));
        // Proceed to next step if application of entites is successful
        this.actions.nextStep.call(this);
      } catch {
        noop();
      }
    },

    /**
     * Action handles wizard step cancellation
     */
    onCancel(this: DatasetCompliance): void {
      this.updateStep(initialStepIndex);
    },

    /**
     * Toggles the flag isShowingFullFieldNames when invoked
     */
    onFieldDblClick(): void {
      this.toggleProperty('isShowingFullFieldNames');
    },

    /**
     * Adds a new field tag to the list of compliance change set items
     * @param {IComplianceChangeSet} tag properties for new field tag
     * @return {IComplianceChangeSet}
     */
    onFieldTagAdded(this: DatasetCompliance, tag: IComplianceChangeSet): void {
      get(this, 'compliancePolicyChangeSet').addObject(tag);
      get(this, 'foldChangeSetTask').perform();
    },

    /**
     * Removes a field tag from the list of compliance change set items
     * @param {IComplianceChangeSet} tag
     * @return {IComplianceChangeSet}
     */
    onFieldTagRemoved(this: DatasetCompliance, tag: IComplianceChangeSet): void {
      get(this, 'compliancePolicyChangeSet').removeObject(tag);
      get(this, 'foldChangeSetTask').perform();
    },

    /**
     * Disables the readonly attribute of a compliance policy changeSet tag,
     * allowing the user to override properties on the tag
     * @param {IComplianceChangeSet} tag the IComplianceChangeSet instance
     */
    async onTagReadOnlyDisable(this: DatasetCompliance, tag: IComplianceChangeSet): Promise<void> {
      const { dialogActions, dismissedOrConfirmed } = notificationDialogActionFactory();
      const {
        doNotShowReadonlyConfirmation,
        notifications: { notify }
      } = getProperties(this, ['doNotShowReadonlyConfirmation', 'notifications']);

      if (doNotShowReadonlyConfirmation) {
        overrideTagReadonly(tag);
        return;
      }

      notify(NotificationEvent.confirm, {
        header: 'Are you sure you would like to modify this field?',
        content:
          "This field's compliance information is currently readonly, please confirm if you would like to override this value",
        dialogActions,
        toggleText: 'Do not show this again for this dataset',
        onDialogToggle: (doNotShow: boolean): boolean => set(this, 'doNotShowReadonlyConfirmation', doNotShow)
      });

      try {
        await dismissedOrConfirmed;
        overrideTagReadonly(tag);
      } catch (e) {
        return;
      }
    },

    /**
     * Applies wholesale user changes to a field tag's properties
     * @param {IComplianceChangeSet} tag a reference to the current tag object
     * @param {IComplianceChangeSet} tagUpdates updated properties to be applied to the current tag
     */
    tagPropertiesUpdated(tag: IComplianceChangeSet, tagUpdates: IComplianceChangeSet) {
      setProperties(tag, tagUpdates);
    },

    /**
     * When a user updates the identifierFieldType, update working copy
     * @param {IComplianceChangeSet} tag
     * @param {ComplianceFieldIdValue} identifierType
     */
    tagIdentifierChanged(
      this: DatasetCompliance,
      tag: IComplianceChangeSet,
      { value: identifierType }: { value: ComplianceFieldIdValue }
    ): void {
      const { identifierField } = tag;
      if (tag) {
        setProperties(tag, {
          identifierType,
          logicalType: null,
          nonOwner: null,
          isDirty: true,
          valuePattern: null
        });
      }

      this.setDefaultClassification({ identifierField, identifierType });
    },

    /**
     * Updates the security classification on a  field tag
     * @param {IComplianceChangeSet} tag the tag to be updated
     * @param {IComplianceChangeSet.securityClassification} securityClassification the updated security classification value
     */
    tagClassificationChanged(
      tag: IComplianceChangeSet,
      { value: securityClassification = null }: { value: IComplianceChangeSet['securityClassification'] }
    ): void {
      setProperties(tag, {
        securityClassification,
        isDirty: true
      });
    },

    /**
     * Sets each datasetClassification value as false
     * @returns {Promise<DatasetClassification>}
     */
    async markDatasetAsNotContainingMemberData(this: DatasetCompliance): Promise<DatasetClassification | void> {
      const { dialogActions, dismissedOrConfirmed: confirmMarkAllSettler } = notificationDialogActionFactory();
      let willMarkAllAsNo = true;

      get(this, 'notifications').notify(NotificationEvent.confirm, {
        content: 'Are you sure this dataset does not contain any of the listed types of data?',
        header: 'Dataset contains no listed types of data',
        dialogActions
      });

      try {
        await confirmMarkAllSettler;
      } catch (e) {
        willMarkAllAsNo = false;
      }

      if (willMarkAllAsNo) {
        return <DatasetClassification>setProperties(
          this.getDatasetClassificationRef(),
          datasetClassifiersKeys.reduce(
            (classification, classifier): {} => ({ ...classification, ...{ [classifier]: false } }),
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
      const option = set(this, 'fieldReviewOption', value);
      get(this, 'foldChangeSetTask').perform();

      return option;
    },

    /**
     * Progresses 1 step backward in the edit sequence
     */
    previousStep(this: DatasetCompliance): void {
      const editStepIndex = get(this, 'editStepIndex');
      const previousIndex = editStepIndex > 0 ? editStepIndex - 1 : editStepIndex;
      this.updateStep(previousIndex);
    },

    /**
     * Progresses 1 step forward in the edit sequence
     */
    nextStep(this: DatasetCompliance): void {
      const { editStepIndex, editSteps } = getProperties(this, ['editStepIndex', 'editSteps']);
      const nextIndex = editStepIndex < editSteps.length - 1 ? editStepIndex + 1 : editStepIndex;
      this.updateStep(nextIndex);
    },

    /**
     * Handler applies fields changeSet working copy to compliance policy to be persisted amd validates fields
     * @returns {Promise<void>}
     */
    async didEditCompliancePolicy(this: DatasetCompliance): Promise<void> {
      // Ensure that the fields on the policy meet the validation criteria before proceeding
      // Otherwise exit early
      try {
        await this.applyWorkingCopy();
        await this.validateFields();
      } catch (e) {
        // Flag this dataset's data as problematic
        if (e instanceof Error && [complianceDataException, complianceFieldNotUnique].includes(e.message)) {
          set(this, '_hasBadData', true);
          window.scrollTo(0, 0);
        }

        throw e;
      }
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
        if (typeof containingPersonalData !== 'boolean') {
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
     * @returns {(Promise<void>)}
     */
    didEditPurgePolicy(this: DatasetCompliance): Promise<void> {
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
     * Augments the tag props with a suggestionAuthority indicating that the tag
     * suggestion has either been accepted or ignored, and assigns the value of that change to the prop
     * @param {IComplianceChangeSet} tag tag for which this suggestion intent should apply
     * @param {SuggestionIntent} [intent=SuggestionIntent.ignore] user's intended action for suggestion, Defaults to `ignore`
     */
    onFieldSuggestionIntentChange(
      this: DatasetCompliance,
      tag: IComplianceChangeSet,
      intent: SuggestionIntent = SuggestionIntent.ignore
    ): void {
      set(tag, 'suggestionAuthority', intent);
    },

    /**
     * Receives the json representation for compliance and applies each key to the policy
     * @param {string} jsonString string representation for the JSON file
     */
    onComplianceJsonUpload(this: DatasetCompliance, jsonString: string): void {
      get(this, 'onComplianceJsonUpdate')(jsonString);
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
    ): DatasetClassification[K] {
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
      const setSaveFlag = (flag = false): boolean => set(this, 'isSaving', flag);

      try {
        const isSaving = true;
        const onSave = get(this, 'onSave');
        setSaveFlag(isSaving);

        await onSave();
        return this.updateStep(-1);
      } finally {
        setSaveFlag();
      }
    },

    // Rolls back changes made to the compliance spec to current
    // server state
    resetCompliance(): void {
      get(this, 'onReset')();
    }
  };
}
