import Component from '@ember/component';
import ComputedProperty, { not, alias } from '@ember/object/computed';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';
import {
  lowQualitySuggestionConfidenceThreshold,
  TagFilter,
  tagsRequiringReview,
  changeSetReviewableAttributeTriggers,
  mergeComplianceEntitiesWithSuggestions,
  asyncMapSchemaColumnPropsToCurrentPrivacyPolicy,
  sortFoldedChangeSetTuples,
  foldComplianceChangeSets,
  tagSuggestionNeedsReview,
  getSupportedPurgePolicies,
  PurgePolicy,
  singleTagsInChangeSet,
  tagsForIdentifierField,
  tagsWithoutIdentifierType,
  ComplianceFieldIdValue,
  NonIdLogicalType,
  overrideTagReadonly,
  getDefaultSecurityClassification,
  SuggestionIntent,
  getFieldIdentifierOptions
} from 'wherehows-web/constants';
import { computed, get, set, getProperties, setProperties } from '@ember/object';
import { arrayMap, compact, iterateArrayAsync, arrayFilter } from 'wherehows-web/utils/array';
import {
  IComplianceChangeSet,
  ISchemaFieldsToPolicy,
  ISchemaFieldsToSuggested,
  IdentifierFieldWithFieldChangeSetTuple,
  IComplianceEntityWithMetadata,
  IComplianceFieldIdentifierOption,
  IDropDownOption
} from 'wherehows-web/typings/app/dataset-compliance';
import { run, next } from '@ember/runloop';
import { readPlatforms } from 'wherehows-web/utils/api/list/platforms';
import { IDatasetColumn } from 'wherehows-web/typings/api/datasets/columns';
import { IColumnFieldProps } from 'wherehows-web/typings/app/dataset-columns';
import { getTagsSuggestions } from 'wherehows-web/utils/datasets/compliance-suggestions';
import { task, waitForProperty } from 'ember-concurrency';
import {
  IComplianceInfo,
  ISuggestedFieldClassification,
  IComplianceSuggestion,
  IComplianceEntity
} from 'wherehows-web/typings/api/datasets/compliance';
import { assert } from '@ember/debug';
import { pluralize } from 'ember-inflector';
import { action } from '@ember-decorators/object';
import { identity } from 'wherehows-web/utils/helpers/functions';
import { IDataPlatform } from 'wherehows-web/typings/api/list/platforms';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { pick } from 'wherehows-web/utils/object';
import { TrackableEventCategory, trackableEvent } from 'wherehows-web/constants/analytics/event-tracking';
import { notificationDialogActionFactory } from 'wherehows-web/utils/notifications/notifications';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';
import { service } from '@ember-decorators/service';

export default class ComplianceSchemaEntities extends Component {
  /**
   * Reference to the application notifications Service
   * @type {ComputedProperty<Notifications>}
   */
  @service
  notifications: Notifications;

  complianceInfo: undefined | IComplianceInfo;
  platform: IDatasetView['platform'];
  isCompliancePolicyAvailable: boolean = false;
  searchTerm: string;

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

  /**
   * Flag determining whether or not we are in an editing state. This is passed in from the
   * dataset-compliance parent
   * @type {boolean}
   */
  isEditing: boolean;

  /**
   * Maybe not useful anymore, used when the dataset has not had any compliance info edits,
   * passed in from the dataset-compliance parent
   * @type {boolean}
   */
  isNewComplianceInfo: boolean;

  /**
   * List of complianceDataType values, passed in from the dataset-compliance parent
   * @type {Array<IComplianceDataType>}
   */
  complianceDataTypes: Array<IComplianceDataType>;

  /**
   * Convenience flag indicating the policy is not currently being edited
   * @type {ComputedProperty<boolean>}
   * @memberof ComplianceSchemaEntities
   */
  isReadOnly: ComputedProperty<boolean> = not('isEditing');

  /**
   * Confidence percentage number used to filter high quality suggestions versus lower quality, passed
   * in from dataset-compliance parent
   * @type {number}
   */
  suggestionConfidenceThreshold: number;

  /**
   * Flag indicating that the field names in each compliance row is truncated or rendered in full
   * @type {boolean}
   */
  isShowingFullFieldNames = true;

  /**
   * Flag indicating the current compliance policy edit-view mode. Guided mode involves fields and dropdowns
   * while the "advanced mode" is a direct json schema edit
   * @type {boolean}
   */
  showGuidedComplianceEditMode: boolean = true;

  /**
   * Flag indicating the readonly confirmation dialog should not be shown again for this compliance form
   * @type {boolean}
   */
  doNotShowReadonlyConfirmation: boolean = false;

  /**
   * Default to show all fields to review
   * @type {string}
   * @memberof ComplianceSchemaEntities
   */
  fieldReviewOption: TagFilter = TagFilter.showAll;

  /**
   * References the ComplianceFieldIdValue enum
   * @type {ComplianceFieldIdValue}
   */
  ComplianceFieldIdValue = ComplianceFieldIdValue;

  /**
   * Reduces the current filtered changeSet to a list of IdentifierFieldWithFieldChangeSetTuple
   * @type {Array<IdentifierFieldWithFieldChangeSetTuple>}
   * @memberof ComplianceSchemaEntities
   */
  foldedChangeSet: Array<IdentifierFieldWithFieldChangeSetTuple>;

  /**
   * The list of supported purge policies for the related platform
   * @type {Array<PurgePolicy>}
   * @memberof ComplianceSchemaEntities
   */
  supportedPurgePolicies: Array<PurgePolicy> = [];

  /**
   * A list of ui values and labels for review filter drop-down
   * @type {Array<{value: string, label:string}>}
   * @memberof ComplianceSchemaEntities
   */
  fieldReviewOptions: Array<{ value: TagFilter; label: string }> = [
    { value: TagFilter.showAll, label: 'Show all fields' },
    { value: TagFilter.showReview, label: 'Show required fields' },
    { value: TagFilter.showSuggested, label: 'Show suggested fields' }
  ];

  schemaFieldNamesMappedToDataTypes: Array<Pick<IDatasetColumn, 'dataType' | 'fieldName'>>;

  /**
   * Computed prop over the current Id fields in the Privacy Policy
   * @type {ISchemaFieldsToPolicy}
   */
  columnIdFieldsToCurrentPrivacyPolicy: ISchemaFieldsToPolicy = {};

  /**
   * Suggested values for compliance types e.g. identifier type and/or logical type
   * @type {IComplianceSuggestion | void}
   */
  complianceSuggestion: IComplianceSuggestion | void;

  notifyOnChangeSetSuggestions: (hasSuggestions: boolean) => void;
  notifyOnChangeSetRequiresReview: (hasChangeSetDrift: boolean) => void;
  notifyOnComplianceSuggestionFeedback: () => void;

  /**
   * The changeSet tags that require user attention
   * @type {ComputedProperty<Array<IComplianceChangeSet>>}
   * @memberof ComplianceSchemaEntities
   */
  changeSetReview = computed(
    `compliancePolicyChangeSet.@each.{${changeSetReviewableAttributeTriggers}}`,
    'complianceDataTypes',
    'suggestionConfidenceThreshold',
    function(this: ComplianceSchemaEntities): Array<IComplianceChangeSet> {
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
   * @memberof ComplianceSchemaEntities
   */
  changeSetReviewCount = alias('changeSetReview.length');

  /**
   * Computes a cta string for the selected field review filter option
   * @type {ComputedProperty<string>}
   * @memberof ComplianceSchemaEntities
   */
  fieldReviewHint: ComputedProperty<string> = computed('fieldReviewOption', 'changeSetReviewCount', function(
    this: ComplianceSchemaEntities
  ): string {
    type TagFilterHint = { [K in TagFilter]: string };

    const { fieldReviewOption, changeSetReviewCount } = getProperties(this, [
      'fieldReviewOption',
      'changeSetReviewCount'
    ]);

    const hint = (<TagFilterHint>{
      [TagFilter.showAll]: `${pluralize(changeSetReviewCount, 'field')} to be reviewed`,
      [TagFilter.showReview]: 'It is required to select compliance info for all fields',
      [TagFilter.showSuggested]: 'Please review suggestions and provide feedback'
    })[fieldReviewOption];

    return changeSetReviewCount ? hint : '';
  });

  /**
   * Creates a mapping of compliance suggestions to identifierField
   * This improves performance in a subsequent merge op since this loop
   * happens only once and is cached
   * @type {ComputedProperty<ISchemaFieldsToSuggested>}
   * @memberof ComplianceSchemaEntities
   */
  identifierFieldToSuggestion = computed('complianceSuggestion', function(
    this: ComplianceSchemaEntities
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
   * @memberof ComplianceSchemaEntities
   */
  compliancePolicyChangeSet = computed(
    'columnIdFieldsToCurrentPrivacyPolicy',
    'complianceDataTypes',
    'identifierFieldToSuggestion',
    'suggestionConfidenceThreshold',
    function(this: ComplianceSchemaEntities): Array<IComplianceChangeSet> {
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
   * @memberof ComplianceSchemaEntities
   */
  filteredChangeSet = computed(
    'changeSetReviewCount',
    'fieldReviewOption',
    'compliancePolicyChangeSet',
    'complianceDataTypes',
    'suggestionConfidenceThreshold',
    function(this: ComplianceSchemaEntities): Array<IComplianceChangeSet> {
      /**
       * Aliases the index signature for a hash of callback functions keyed by TagFilter
       * to filter out compliance changeset items
       * @alias
       */
      type TagFilterCallback<T = Array<IComplianceChangeSet>> = { [K in TagFilter]: (x: T) => T };

      const {
        compliancePolicyChangeSet: changeSet,
        complianceDataTypes,
        suggestionConfidenceThreshold
      } = getProperties(this, ['compliancePolicyChangeSet', 'complianceDataTypes', 'suggestionConfidenceThreshold']);

      // references the filter predicate for changeset items based on the currently set tag filter
      const changeSetFilter = (<TagFilterCallback>{
        [TagFilter.showAll]: identity,
        [TagFilter.showReview]: tagsRequiringReview(complianceDataTypes, {
          checkSuggestions: true,
          suggestionConfidenceThreshold
        }),
        [TagFilter.showSuggested]: arrayFilter((tag: IComplianceChangeSet) =>
          tagSuggestionNeedsReview({ ...tag, suggestionConfidenceThreshold })
        )
      })[get(this, 'fieldReviewOption')];

      return changeSetFilter(changeSet);
    }
  );

  /**
   * Lists the IComplianceChangeSet / tags without an identifierType value
   * @type {ComputedProperty<Array<IComplianceChangeSet>>}
   * @memberof ComplianceSchemaEntities
   */
  unspecifiedTags = computed(`compliancePolicyChangeSet.@each.{${changeSetReviewableAttributeTriggers}}`, function(
    this: ComplianceSchemaEntities
  ): Array<IComplianceChangeSet> {
    const tags = get(this, 'compliancePolicyChangeSet');
    const singleTags = singleTagsInChangeSet(tags, tagsForIdentifierField);

    return tagsWithoutIdentifierType(singleTags);
  });

  /**
   * Formatted JSON string representing the compliance entities for this dataset
   * @type {ComputedProperty<string>}
   */
  jsonComplianceEntities: ComputedProperty<string> = computed('columnIdFieldsToCurrentPrivacyPolicy', function(
    this: ComplianceSchemaEntities
  ): string {
    const entityAttrs: Array<keyof IComplianceEntity> = [
      'identifierField',
      'identifierType',
      'logicalType',
      'nonOwner',
      'valuePattern',
      'readonly'
    ];
    const entityMap: ISchemaFieldsToPolicy = get(this, 'columnIdFieldsToCurrentPrivacyPolicy');
    const entitiesWithModifiableKeys = arrayMap((tag: IComplianceEntityWithMetadata) => pick(tag, entityAttrs))(
      (<Array<IComplianceEntityWithMetadata>>[]).concat(...Object.values(entityMap))
    );

    return JSON.stringify(entitiesWithModifiableKeys, null, '\t');
  });

  /**
   * Reads the complianceDataTypes property and transforms into a list of drop down options for the field
   * identifier type
   * @type {ComputedProperty<Array<IComplianceFieldIdentifierOption  | IDropDownOption<null | 'NONE'>>>}
   */
  complianceFieldIdDropdownOptions = computed('complianceDataTypes', function(
    this: ComplianceSchemaEntities
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
   * Task to retrieve column fields async and set values on Component
   * @type {Task<Promise<any>, () => TaskInstance<Promise<any>>>}
   * @memberof ComplianceSchemaEntities
   */
  columnFieldsToCompliancePolicyTask = task(function*(this: ComplianceSchemaEntities): IterableIterator<any> {
    // Truncated list of Dataset field names and data types currently returned from the column endpoint
    const schemaFieldNamesMappedToDataTypes: ComplianceSchemaEntities['schemaFieldNamesMappedToDataTypes'] = yield waitForProperty(
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
    this: ComplianceSchemaEntities,
    { identifierField, identifierType }: Pick<IComplianceEntity, 'identifierField' | 'identifierType'>
  ): void {
    const complianceDataTypes = get(this, 'complianceDataTypes');
    const defaultSecurityClassification = getDefaultSecurityClassification(complianceDataTypes, identifierType);

    this.actions.tagClassificationChanged.call(this, { identifierField }, { value: defaultSecurityClassification });
  }

  /**
   * Task to retrieve platform policies and set supported policies for the current platform
   * @type {Task<Promise<any>, () => TaskInstance<Promise<any>>>}
   * @memberof ComplianceSchemaEntities
   */
  foldChangeSetTask = task(function*(this: ComplianceSchemaEntities): IterableIterator<any> {
    //@ts-ignore dot notation for property access
    yield waitForProperty(this, 'columnFieldsToCompliancePolicyTask.isIdle');
    const filteredChangeSet = get(this, 'filteredChangeSet');
    const foldedChangeSet: Array<IdentifierFieldWithFieldChangeSetTuple> = yield foldComplianceChangeSets(
      filteredChangeSet
    );

    set(this, 'foldedChangeSet', sortFoldedChangeSetTuples(foldedChangeSet));
  }).enqueue();

  /**
   * Task to retrieve platform policies and set supported policies for the current platform
   * @type {Task<Promise<Array<IDataPlatform>>, () => TaskInstance<Promise<Array<IDataPlatform>>>>}
   * @memberof ComplianceSchemaEntities
   */
  getPlatformPoliciesTask = task(function*(
    this: ComplianceSchemaEntities
  ): IterableIterator<Promise<Array<IDataPlatform>>> {
    const platform = get(this, 'platform');

    if (platform) {
      set(this, 'supportedPurgePolicies', getSupportedPurgePolicies(platform, yield readPlatforms()));
    }
  }).restartable();

  /**
   * Sets the identifierType attribute on IComplianceChangeSetFields without an identifierType to ComplianceFieldIdValue.None
   * @returns {Promise<Array<IComplianceChangeSet>>}
   */
  setUnspecifiedTagsAsNoneTask = task(function*(
    this: ComplianceSchemaEntities
  ): IterableIterator<Promise<Array<ComplianceFieldIdValue | NonIdLogicalType>>> {
    const unspecifiedTags = get(this, 'unspecifiedTags');
    const setTagIdentifier = (value: ComplianceFieldIdValue | NonIdLogicalType) => (tag: IComplianceChangeSet) =>
      set(tag, 'identifierType', value);

    yield iterateArrayAsync(arrayMap(setTagIdentifier(ComplianceFieldIdValue.None)))(unspecifiedTags);
  }).drop();

  constructor() {
    super(...arguments);
    // Setting default values for passed in properties
    typeof this.isEditing === 'boolean' || (this.isEditing = false);
    typeof this.isNewComplianceInfo === 'boolean' || (this.isNewComplianceInfo = false);
    this.searchTerm || set(this, 'searchTerm', '');
    typeof this.suggestionConfidenceThreshold === 'number' ||
      (this.suggestionConfidenceThreshold = lowQualitySuggestionConfidenceThreshold);
    this.complianceDataTypes || (this.complianceDataTypes = []);
    // this.schemaFieldNamesMappedToDataTypes || (this.schemaFieldNamesMappedToDataTypes = []);
  }

  didInsertElement(): void {
    get(this, 'columnFieldsToCompliancePolicyTask').perform();
    get(this, 'foldChangeSetTask').perform();
  }

  didUpdateAttrs(): void {
    get(this, 'columnFieldsToCompliancePolicyTask').perform();
    get(this, 'foldChangeSetTask').perform();
  }

  /**
   * Updates the fieldReviewOption with the user selected value
   * @param {{value: TagFilter}} { value }
   * @returns {TagFilter}
   */
  @action
  onFieldReviewChange(this: ComplianceSchemaEntities, { value }: { value: TagFilter }): TagFilter {
    const option = set(this, 'fieldReviewOption', value);
    get(this, 'foldChangeSetTask').perform();

    return option;
  }

  /**
   * Toggles the flag isShowingFullFieldNames when invoked
   */
  @action
  onFieldDblClick(): void {
    this.toggleProperty('isShowingFullFieldNames');
  }

  /**
   * Adds a new field tag to the list of compliance change set items
   * @param {IComplianceChangeSet} tag properties for new field tag
   * @return {IComplianceChangeSet}
   */
  @action
  onFieldTagAdded(this: ComplianceSchemaEntities, tag: IComplianceChangeSet): void {
    get(this, 'compliancePolicyChangeSet').addObject(tag);
    get(this, 'foldChangeSetTask').perform();
  }

  /**
   * Removes a field tag from the list of compliance change set items
   * @param {IComplianceChangeSet} tag
   * @return {IComplianceChangeSet}
   */
  @action
  onFieldTagRemoved(this: ComplianceSchemaEntities, tag: IComplianceChangeSet): void {
    get(this, 'compliancePolicyChangeSet').removeObject(tag);
    get(this, 'foldChangeSetTask').perform();
  }

  /**
   * Disables the readonly attribute of a compliance policy changeSet tag,
   * allowing the user to override properties on the tag
   * @param {IComplianceChangeSet} tag the IComplianceChangeSet instance
   */
  @action
  async onTagReadOnlyDisable(this: ComplianceSchemaEntities, tag: IComplianceChangeSet): Promise<void> {
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
  }

  /**
   * When a user updates the identifierFieldType, update working copy
   * @param {IComplianceChangeSet} tag
   * @param {ComplianceFieldIdValue} identifierType
   */
  @action
  tagIdentifierChanged(
    this: ComplianceSchemaEntities,
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
  }

  /**
   * Augments the tag props with a suggestionAuthority indicating that the tag
   * suggestion has either been accepted or ignored, and assigns the value of that change to the prop
   * @param {IComplianceChangeSet} tag tag for which this suggestion intent should apply
   * @param {SuggestionIntent} [intent=SuggestionIntent.ignore] user's intended action for suggestion, Defaults to `ignore`
   */
  @action
  onFieldSuggestionIntentChange(
    this: ComplianceSchemaEntities,
    tag: IComplianceChangeSet,
    intent: SuggestionIntent = SuggestionIntent.ignore
  ): void {
    set(tag, 'suggestionAuthority', intent);
  }

  /**
   * Applies wholesale user changes to a field tag's properties
   * @param {IComplianceChangeSet} tag a reference to the current tag object
   * @param {IComplianceChangeSet} tagUpdates updated properties to be applied to the current tag
   */
  @action
  tagPropertiesUpdated(tag: IComplianceChangeSet, tagUpdates: IComplianceChangeSet) {
    setProperties(tag, tagUpdates);
  }

  /**
   * Toggle the visibility of the guided compliance edit view vs the advanced edit view modes
   * @param {boolean} toggle flag ,if true, show guided edit mode, otherwise, advanced
   */
  @action
  onShowGuidedEditMode(this: ComplianceSchemaEntities, toggle: boolean): void {
    const isShowingGuidedEditMode = set(this, 'showGuidedComplianceEditMode', toggle);

    if (!isShowingGuidedEditMode) {
      this.actions.onManualComplianceUpdate.call(this, get(this, 'jsonComplianceEntities'));
    }
  }
}
