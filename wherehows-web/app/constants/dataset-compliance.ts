import { setProperties } from '@ember/object';
import { IdLogicalType, PurgePolicy } from 'wherehows-web/constants/index';
import { IComplianceEntity, IComplianceInfo } from 'wherehows-web/typings/api/datasets/compliance';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';
import { arrayEvery, arrayFilter, arrayMap, arrayReduce, arraySome, reduceArrayAsync } from 'wherehows-web/utils/array';
import { fleece, hasEnumerableKeys } from 'wherehows-web/utils/object';
import { lastSeenSuggestionInterval } from 'wherehows-web/constants/metadata-acquisition';
import { decodeUrn, isValidCustomValuePattern } from 'wherehows-web/utils/validators/urn';
import {
  IComplianceChangeSet,
  IComplianceFieldIdentifierOption,
  IdentifierFieldWithFieldChangeSetTuple,
  IIdentifierFieldWithFieldChangeSetObject,
  ISchemaFieldsToPolicy,
  ISchemaFieldsToSuggested,
  IComplianceEntityWithMetadata,
  ISuggestedFieldTypeValues,
  IComplianceTagReviewOptions
} from 'wherehows-web/typings/app/dataset-compliance';
import {
  IColumnFieldProps,
  ISchemaColumnMappingProps,
  ISchemaWithPolicyTagsReducingFn
} from 'wherehows-web/typings/app/dataset-columns';
import { IDatasetColumn } from 'wherehows-web/typings/api/datasets/columns';
import { ComplianceFieldIdValue } from 'wherehows-web/constants/datasets/compliance';
import { isHighConfidenceSuggestion } from 'wherehows-web/utils/datasets/compliance-suggestions';
import { validateRegExp } from 'wherehows-web/utils/validators/regexp';

/**
 * Defines a map of values for the compliance policy on a dataset
 * @type {object}
 */
const compliancePolicyStrings = {
  // TODO:  DSS-6122 Create and move to Error module
  complianceDataException: 'Unexpected discrepancy in compliance data.',
  complianceFieldNotUnique: 'Compliance fields have failed to verify a uniqueness check.',
  missingTypes: 'Looks like you may have forgotten to specify a `Field Format` for all ID fields?',
  successUpdating: 'Changes have been successfully saved!',
  failedUpdating: 'An error occurred while saving.',
  successUploading: 'Metadata successfully updated! Please confirm and complete subsequent metadata information.',
  invalidPolicyData: 'Received policy in an unexpected format! Please check the provided attributes and try again.',
  missingPurgePolicy: 'Please specify a Compliance Purge Policy',
  missingDatasetSecurityClassification: 'Please specify a security classification for this dataset.'
};

/**
 * Field / changeSet attributes that will trigger a check if review is requested
 * field `logicalType` in `changeSetReviewableAttributeTriggers` is used in the determination of idType fields
 * without a logicalType as requiring review
 * @type {string}
 */
const changeSetReviewableAttributeTriggers =
  'isDirty,suggestion,privacyPolicyExists,suggestionAuthority,logicalType,identifierType,nonOwner,valuePattern,readonly';

/**
 * Takes a compliance data type and transforms it into a compliance field identifier option
 * @param {IComplianceDataType} complianceDataType
 * @returns {IComplianceFieldIdentifierOption}
 */
const getFieldIdentifierOption = (complianceDataType: IComplianceDataType): IComplianceFieldIdentifierOption => {
  const { id, title, idType } = complianceDataType;
  return { value: id, label: title, isId: idType };
};

/**
 * Maps over a list of compliance data types objects and transforms to a list of dropdown options
 * @type {(array: Array<IComplianceDataType>) => Array<IComplianceFieldIdentifierOption>}
 */
const getFieldIdentifierOptions = arrayMap(getFieldIdentifierOption);

/**
 * Defines the sequence of edit steps in the compliance policy component
 */
const complianceSteps = {
  0: {
    name: 'editCompliancePolicy'
  },
  1: {
    name: 'editPurgePolicy'
  },
  2: {
    name: 'editDatasetClassification'
  }
};

/**
 * Takes a map of dataset options and constructs the relevant compliance edit wizard steps to build the wizard flow
 * @param {boolean} [hasSchema=true] flag indicating if the dataset has a schema or otherwise
 * @returns {({ [x: number]: { name: string } })}
 */
const getComplianceSteps = (hasSchema: boolean = true): { [x: number]: { name: string } } => {
  // Step to tag dataset with PII data, this is at the dataset level for schema-less datasets
  const piiTaggingStep = { 0: { name: 'editDatasetLevelCompliancePolicy' } };

  if (!hasSchema) {
    return { ...complianceSteps, ...piiTaggingStep };
  }

  return complianceSteps;
};

/**
 * Returns true if argument of type IComplianceEntity has its readonly attribute not set to true
 * @param {IComplianceEntity} { readonly }
 * @returns {boolean}
 */
const isEditableTag = ({ readonly }: IComplianceEntity): boolean => readonly !== true; // do not simplify, readonly may be undefined

/**
 * Filters out from a list of compliance entities, entities that are editable
 * @param {Array<IComplianceEntity>} entities
 * @returns {Array<IComplianceEntity>}
 */
const editableTags = (entities: Array<IComplianceEntity>): Array<IComplianceEntity> =>
  arrayFilter(isEditableTag)(entities);

/**
 * Strips out the readonly attribute from a list of compliance entities
 * @type {(entities: Array<IComplianceEntity>) => Array<IComplianceEntity>}
 */
const removeReadonlyAttr = <(entities: Array<IComplianceEntity>) => Array<IComplianceEntity>>arrayMap(
  fleece<IComplianceEntity, 'readonly'>(['readonly'])
);

/**
 * Determines if an IComplianceInfo object is auto-generated
 * @param {IComplianceInfo} [policy] the compliance policy
 * @returns {boolean}
 */
const isAutoGeneratedPolicy = (policy?: IComplianceInfo): boolean => {
  if (policy) {
    const { complianceType, complianceEntities: { length } = [] } = policy;

    return !length && Object.values(PurgePolicy).includes(complianceType);
  }

  return false;
};

/**
 * Takes a list of compliance data types and maps a list of compliance id's with idType set to true
 * @param {Array<IComplianceDataType>} [complianceDataTypes=[]] the list of compliance data types to transform
 * @return {Array<ComplianceFieldIdValue>}
 */
const getIdTypeDataTypes = (complianceDataTypes: Array<IComplianceDataType> = []): Array<string> =>
  complianceDataTypes.filter(complianceDataType => complianceDataType.idType).mapBy('id');

/**
 * Checks if the compliance suggestion has a date that is equal or exceeds the policy mod time by at least the
 * ms time in lastSeenSuggestionInterval
 * @param {IComplianceInfo.modifiedTime} policyModificationTime timestamp for the policy modification date
 * @param {number} suggestionModificationTime timestamp for the suggestion modification date
 * @return {boolean}
 */
const isRecentSuggestion = (
  policyModificationTime: IComplianceInfo['modifiedTime'],
  suggestionModificationTime: number
): boolean =>
  // policy has not been modified previously or suggestion mod time is greater than or equal to interval
  !policyModificationTime ||
  (!!suggestionModificationTime &&
    suggestionModificationTime - parseInt(policyModificationTime) >= lastSeenSuggestionInterval);

/**
 * Takes a suggestion and populates a list of ComplianceFieldIdValues if the suggestion matches the identifier
 * @param {ISuggestedFieldTypeValues | void} suggestion
 * @return {(list: Array<ComplianceFieldIdValue>, identifierType: ComplianceFieldIdValue) => Array<ComplianceFieldIdValue>}
 */
const suggestedIdentifierTypesInList = (suggestion: ISuggestedFieldTypeValues | void) => (
  list: Array<IComplianceEntity['identifierType']>,
  identifierType: IComplianceEntity['identifierType']
): Array<IComplianceEntity['identifierType']> =>
  suggestion && suggestion.identifierType === identifierType ? [...list, identifierType] : list;

/**
 * Checks if a tag (IComplianceChangeSet) instance should be reviewed for conflict with the suggested value
 * @param {SchemaFieldToSuggestedValue} suggestion
 * @param {SuggestionIntent} suggestionAuthority
 * @param {ComplianceFieldIdValue | NonIdLogicalType | null} identifierType
 * @param {number} suggestionConfidenceThreshold confidence threshold for filtering out higher quality suggestions
 * @return {boolean}
 */
const tagSuggestionNeedsReview = ({
  suggestion,
  suggestionAuthority,
  identifierType,
  suggestionConfidenceThreshold
}: IComplianceChangeSet & { suggestionConfidenceThreshold: number }): boolean =>
  suggestion &&
  suggestion.identifierType !== identifierType &&
  isHighConfidenceSuggestion(suggestion, suggestionConfidenceThreshold)
    ? !suggestionAuthority
    : false;

/**
 * Checks if a compliance tag's logicalType property is missing or nonOwner flag is set
 * @param {IComplianceChangeSet} tag the compliance field tag
 * @return {boolean}
 */
const tagLogicalTypeNonOwnerAttributeNeedsReview = (tag: IComplianceChangeSet): boolean =>
  !idTypeTagHasLogicalType(tag) || !tagOwnerIsSet(tag);

/**
 * Checks if a compliance tag's valuePattern attribute is valid or otherwise
 * @param {string | null} valuePattern
 * @return {boolean}
 */
const tagValuePatternNeedsReview = ({ valuePattern }: IComplianceChangeSet): boolean => {
  let isValid = false;
  try {
    ({ isValid } = validateRegExp(valuePattern, isValidCustomValuePattern));
  } catch {
    isValid = false;
  }

  return !isValid;
};

/**
 * Checks if a compliance policy changeSet field requires user attention: if a suggestion
 * is available  but the user has not indicated intent or a policy for the field does not currently exist remotely
 * and the related field changeSet has not been modified on the client and isn't readonly
 * @param {Array<IComplianceDataType>} complianceDataTypes
 * @param {IComplianceTagReviewOptions} options an option bag with properties that modify the behaviour of the review
 * checking steps
 * @return {(tag: IComplianceChangeSet) => boolean}
 */
const tagNeedsReview = (complianceDataTypes: Array<IComplianceDataType>, options: IComplianceTagReviewOptions) =>
  /**
   * Checks if a compliance tag needs to be reviewed against a set of rules
   * @param {IComplianceChangeSet} tag
   * @return {boolean}
   */
  (tag: IComplianceChangeSet): boolean => {
    const { checkSuggestions, suggestionConfidenceThreshold } = options;
    const { isDirty, privacyPolicyExists, identifierType, logicalType } = tag;
    let isReviewRequired = false;

    // Ensure that the tag has an identifier type specified
    if (!identifierType) {
      return true;
    }

    // Check that a hi confidence suggestion exists and the identifierType does not match the change set item
    if (checkSuggestions) {
      isReviewRequired = isReviewRequired || tagSuggestionNeedsReview({ ...tag, suggestionConfidenceThreshold });
    }

    // Ensure that tag has a logical type and nonOwner flag is set when tag is of id type
    if (isTagIdType(complianceDataTypes)(tag)) {
      isReviewRequired = isReviewRequired || tagLogicalTypeNonOwnerAttributeNeedsReview(tag);
    }

    // If the tag has a IdLogicalType.Custom logicalType, check that the value pattern is truthy
    if (logicalType === IdLogicalType.Custom) {
      isReviewRequired = isReviewRequired || tagValuePatternNeedsReview(tag);
    }

    // If either the privacy policy doesn't exists, or user hasn't made changes, then review is required
    return isReviewRequired || !(privacyPolicyExists || isDirty);
  };

/**
 * Asserts that a compliance field tag has an identifierType of ComplianceFieldIdValue.None
 * @param {ComplianceFieldIdValue | NonIdLogicalType | null} identifierType
 * @return {boolean}
 */
const isTagNoneType = ({ identifierType }: IComplianceChangeSet): boolean =>
  identifierType === ComplianceFieldIdValue.None;

/**
 * Checks if a tag has an identifier type
 * @param {ComplianceFieldIdValue | NonIdLogicalType | null} identifierType
 * @return {boolean}
 */
const isTagWithoutIdentifierType = ({ identifierType }: IComplianceChangeSet): boolean => !identifierType;

/**
 * Filters a list of tags without an identifier type value
 * @param {Array<IComplianceChangeSet>} tags
 * @return {Array<IComplianceChangeSet>}
 */
const tagsWithoutIdentifierType = (tags: Array<IComplianceChangeSet>): Array<IComplianceChangeSet> =>
  arrayFilter(isTagWithoutIdentifierType)(tags);

/**
 * Asserts the inverse of isTagNoneType
 * @param {IComplianceChangeSet} tag
 * @return {boolean}
 */
const isTagNotNoneType = (tag: IComplianceChangeSet): boolean => !isTagNoneType(tag);

/**
 * Asserts that a list of tags have at least 1 type of ComplianceFieldIdValue.None
 * @type {(array: Array<IComplianceChangeSet>) => boolean}
 */
const tagsHaveNoneType = arraySome(isTagNoneType);

/**
 * Asserts that a list of tags have at least 1 type that is not of ComplianceFieldIdValue.None
 * @type {(array: Array<IComplianceChangeSet>) => boolean}
 */
const tagsHaveNotNoneType = arraySome(isTagNotNoneType);

/**
 * Asserts that a list of tags have a type of ComplianceFieldIdValue.None and not ComplianceFieldIdValue.None
 * @param {Array<IComplianceChangeSet>} tags
 * @return {boolean}
 */
const tagsHaveNoneAndNotNoneType = (tags: Array<IComplianceChangeSet>) =>
  tagsHaveNoneType(tags) && tagsHaveNotNoneType(tags);

/**
 * Asserts that a tag / change set item has an identifier type that is in the list of compliance data types
 * @param {Array<IComplianceDataType>} [complianceDataTypes=[]]
 */
const isTagIdType = (complianceDataTypes: Array<IComplianceDataType> = []) => ({
  identifierType
}: IComplianceChangeSet): boolean => getIdTypeDataTypes(complianceDataTypes).includes(<string>identifierType);

/**
 * Asserts that a tag has a value for it's identifierType property and the identifierType is not ComplianceFieldIdValue.None
 * @param {IComplianceChangeSet} {identifierType}
 * @returns {boolean}
 */
const tagHasIdentifierType = ({ identifierType }: IComplianceChangeSet): boolean =>
  !!identifierType && identifierType !== ComplianceFieldIdValue.None;

/**
 * Takes an array of compliance change sets and checks that each item has an identifierType
 * @type {(array: IComplianceChangeSet[]) => boolean}
 */
const fieldTagsHaveIdentifierType = arrayEvery(tagHasIdentifierType);

/**
 * Asserts that a compliance entity has a logical type
 * @param {IComplianceEntity} { logicalType }
 * @returns {boolean}
 */
const idTypeTagHasLogicalType = ({ logicalType }: Pick<IComplianceEntity, 'logicalType'>): boolean => !!logicalType;

const tagOwnerIsSet = ({ nonOwner }: Pick<IComplianceChangeSet, 'nonOwner'>): boolean => typeof nonOwner === 'boolean';

/**
 * Asserts that a list of compliance entities each have a logicalType attribute value
 * @type {(array: IComplianceEntity[]) => boolean}
 */
const idTypeTagsHaveLogicalType = arrayEvery(idTypeTagHasLogicalType);

/**
 * Describes the function interface for tagsForIdentifierField
 * @interface ITagsForIdentifierFieldFn
 */
interface ITagsForIdentifierFieldFn {
  (identifierField: string): (tags: Array<IComplianceChangeSet>) => Array<IComplianceChangeSet>;
}
/**
 * Gets the tags for a specific identifier field
 * @param {string} identifierField
 * @return {ITagsForIdentifierFieldFn}
 */
const tagsForIdentifierField: ITagsForIdentifierFieldFn = (identifierField: string) =>
  arrayFilter(isSchemaFieldTag<IComplianceChangeSet>(identifierField));

/**
 * Lists tags that occur for only one identifier type in the list of tags
 * @param {Array<IComplianceChangeSet>} tags the full list of tags to iterate through
 * @param {ITagsForIdentifierFieldFn} tagsForIdentifierFieldFn
 * @return {(singleTags: Array<IComplianceChangeSet>, { identifierField }: IComplianceChangeSet) => (any)[] | Array<IComplianceChangeSet>}
 */
const singleTagsIn = (tags: Array<IComplianceChangeSet>, tagsForIdentifierFieldFn: ITagsForIdentifierFieldFn) => (
  singleTags: Array<IComplianceChangeSet>,
  { identifierField }: IComplianceChangeSet
): Array<IComplianceChangeSet> => {
  const tagsForIdentifier = tagsForIdentifierFieldFn(identifierField)(tags);
  return tagsForIdentifier.length === 1 ? [...singleTags, ...tagsForIdentifier] : singleTags;
};

/**
 * Lists the tags in a list of tags that occur for only one identifier type
 * @param {Array<IComplianceChangeSet>} tags
 * @param {ITagsForIdentifierFieldFn} tagsForIdentifierFieldFn
 * @return {Array<IComplianceChangeSet>}
 */
const singleTagsInChangeSet = (
  tags: Array<IComplianceChangeSet>,
  tagsForIdentifierFieldFn: ITagsForIdentifierFieldFn
): Array<IComplianceChangeSet> => arrayReduce(singleTagsIn(tags, tagsForIdentifierFieldFn), [])(tags);

/**
 * Returns a list of changeSet tags that requires user attention
 * @param {Array<IComplianceDataType>} complianceDataTypes
 * @param {IComplianceTagReviewOptions} options
 * @return {(array: Array<IComplianceChangeSet>) => Array<IComplianceChangeSet>}
 */
const tagsRequiringReview = (complianceDataTypes: Array<IComplianceDataType>, options: IComplianceTagReviewOptions) =>
  arrayFilter<IComplianceChangeSet>(tagNeedsReview(complianceDataTypes, options));

/**
 * Lists the tags for a specific identifier field that need to be reviewed
 * @param {Array<IComplianceDataType>} complianceDataTypes
 * @param {IComplianceTagReviewOptions} options
 * @return {(identifierField: string) => (tags: Array<IComplianceChangeSet>) => Array<IComplianceChangeSet>}
 */
const fieldTagsRequiringReview = (
  complianceDataTypes: Array<IComplianceDataType>,
  options: IComplianceTagReviewOptions
) => (identifierField: string) => (tags: Array<IComplianceChangeSet>) =>
  tagsRequiringReview(complianceDataTypes, options)(tagsForIdentifierField(identifierField)(tags));

/**
 * Extracts a suggestion for a field from a suggestion map and merges a compliance entity with the suggestion
 * @param {ISchemaFieldsToSuggested} suggestionMap a hash of compliance fields to suggested values
 * @return {(entity: IComplianceEntityWithMetadata) => IComplianceChangeSet}
 */
const complianceEntityWithSuggestions = (suggestionMap: ISchemaFieldsToSuggested) => (
  entity: IComplianceEntityWithMetadata
): IComplianceChangeSet => {
  const { identifierField, policyModificationTime } = entity;
  const suggestion = suggestionMap[identifierField];

  return suggestion && isRecentSuggestion(policyModificationTime, suggestion.suggestionsModificationTime)
    ? { ...entity, suggestion }
    : entity;
};

/**
 * Creates a list of IComplianceChangeSet instances by merging compliance entities with the related suggestions for
 * the identifier field
 * @param {ISchemaFieldsToPolicy} [schemaEntityMap={}] a map of fields on the dataset schema to the current compliance
 * entities found on the policy
 * @param {ISchemaFieldsToSuggested} [suggestionMap={}] map of fields on the dataset schema to suggested compliance
 * values
 * @returns {Array<IComplianceChangeSet>}
 */
const mergeComplianceEntitiesWithSuggestions = (
  schemaEntityMap: ISchemaFieldsToPolicy = {},
  suggestionMap: ISchemaFieldsToSuggested = {}
): Array<IComplianceChangeSet> =>
  arrayMap(complianceEntityWithSuggestions(suggestionMap))([].concat.apply([], Object.values(schemaEntityMap)));

/**
 * Creates a map of compliance changeSet identifier field to compliance change sets
 * @param {IIdentifierFieldWithFieldChangeSetObject} identifierFieldMap
 * @param {IComplianceChangeSet} changeSet
 * @returns {IIdentifierFieldWithFieldChangeSetObject}
 */
const foldComplianceChangeSetToField = (
  identifierFieldMap: IIdentifierFieldWithFieldChangeSetObject,
  changeSet: IComplianceChangeSet
): IIdentifierFieldWithFieldChangeSetObject => ({
  ...identifierFieldMap,
  [changeSet.identifierField]: [...(identifierFieldMap[changeSet.identifierField] || []), changeSet]
});

/**
 * Reduces a list of IComplianceChangeSet to a list of tuples with a complianceChangeSet identifierField
 * and a changeSet list
 * @param {Array<IComplianceChangeSet>} changeSet
 * @returns {Array<IdentifierFieldWithFieldChangeSetTuple>}
 */
const foldComplianceChangeSets = async (
  changeSet: Array<IComplianceChangeSet>
): Promise<Array<IdentifierFieldWithFieldChangeSetTuple>> =>
  Object.entries<Array<IComplianceChangeSet>>(
    await reduceArrayAsync(arrayReduce(foldComplianceChangeSetToField, {}))(changeSet)
  );

/**
 * Builds a default shape for securitySpecification & privacyCompliancePolicy with default / unset values
 *   for non null properties as per Avro schema
 * @param {string} datasetUrn identifier for the dataset that this privacy object applies to
 * @return {IComplianceInfo}
 */
const initialComplianceObjectFactory = (datasetUrn: string): IComplianceInfo => ({
  datasetUrn: decodeUrn(datasetUrn),
  datasetId: null,
  confidentiality: null,
  complianceType: '',
  compliancePurgeNote: '',
  complianceEntities: [],
  datasetClassification: null
});

/**
 * Maps the fields found in the column property on the schema api to the values returned in the current privacy policy
 * @param {ISchemaColumnMappingProps} {
 *   columnProps,
 *   complianceEntities,
 *   policyModificationTime
 * }
 * @returns {ISchemaFieldsToPolicy}
 */
const asyncMapSchemaColumnPropsToCurrentPrivacyPolicy = ({
  columnProps,
  complianceEntities,
  policyModificationTime
}: ISchemaColumnMappingProps): Promise<ISchemaFieldsToPolicy> =>
  reduceArrayAsync(arrayReduce(schemaFieldsWithPolicyTagsReducingFn(complianceEntities, policyModificationTime), {}))(
    columnProps
  );

/**
 * Creates a new tag / change set item for a compliance entity / field with default properties
 * @param {IColumnFieldProps} { identifierField, dataType } the runtime properties to apply to the created instance
 * @returns {IComplianceEntityWithMetadata}
 */
const complianceFieldChangeSetItemFactory = ({
  identifierField,
  dataType,
  identifierType,
  logicalType,
  suggestion,
  suggestionAuthority
}: IColumnFieldProps): IComplianceChangeSet =>
  Object.assign(
    {
      identifierField,
      dataType,
      identifierType: identifierType || null,
      logicalType: logicalType || null,
      securityClassification: null,
      nonOwner: null,
      readonly: false,
      privacyPolicyExists: false,
      isDirty: true,
      valuePattern: null
    },
    suggestion ? { suggestion } : void 0,
    suggestionAuthority ? { suggestionAuthority } : void 0
  );

/**
 * Asserts that a schema field name matches the compliance entity supplied later
 * @param {string} identifierFieldMatch the field name to match to the IComplianceEntity
 * @return {({ identifierField }: IComplianceEntity) => boolean}
 */
const isSchemaFieldTag = <T extends { identifierField: string }>(identifierFieldMatch: string) => ({
  identifierField
}: T): boolean => identifierFieldMatch === identifierField;

/**
 * Creates an instance of a compliance entity with client side metadata about the entity
 * @param {IComplianceInfo.modifiedTime} policyLastModified time the compliance policy was last modified
 * @param {IDatasetColumn.dataType} dataType the field data type
 * @return {(arg: IComplianceEntity) => IComplianceEntityWithMetadata}
 */
const complianceEntityWithMetadata = (
  policyLastModified: IComplianceInfo['modifiedTime'],
  dataType: IDatasetColumn['dataType']
): ((arg: IComplianceEntity) => IComplianceEntityWithMetadata) => (
  tag: IComplianceEntity
): IComplianceEntityWithMetadata => ({
  ...tag,
  policyModificationTime: policyLastModified,
  dataType,
  privacyPolicyExists: hasEnumerableKeys(tag),
  isDirty: false
});

/**
 * Takes the current compliance entities, and mod time and returns a reducer that consumes a list of IColumnFieldProps
 * instances and maps each entry to a compliance entity on the current compliance policy
 * @param {IComplianceInfo.complianceEntities} currentEntities
 * @param {IComplianceInfo.modifiedTime} policyModificationTime
 * @return {(schemaFieldsToPolicy: ISchemaFieldsToPolicy, { identifierField, dataType}: IColumnFieldProps) => ISchemaFieldsToPolicy}
 */
const schemaFieldsWithPolicyTagsReducingFn: ISchemaWithPolicyTagsReducingFn = (
  currentEntities: IComplianceInfo['complianceEntities'],
  policyModificationTime: IComplianceInfo['modifiedTime']
) => (
  schemaFieldsToPolicy: ISchemaFieldsToPolicy,
  { identifierField, dataType }: IColumnFieldProps
): ISchemaFieldsToPolicy => {
  let complianceEntitiesWithMetadata: Array<IComplianceEntityWithMetadata>;
  let schemaFieldTags = arrayFilter(isSchemaFieldTag<IComplianceEntity>(identifierField))(currentEntities);

  schemaFieldTags = schemaFieldTags.length ? schemaFieldTags : [complianceFieldTagFactory(identifierField)];
  complianceEntitiesWithMetadata = arrayMap(complianceEntityWithMetadata(policyModificationTime, dataType))(
    schemaFieldTags
  );

  return { ...schemaFieldsToPolicy, [identifierField]: complianceEntitiesWithMetadata };
};

/**
 * Constructs an instance of IComplianceEntity with default values, and an identifierField
 * @param {IComplianceEntity.identifierField} identifierField
 * @return {IComplianceEntity}
 */
const complianceFieldTagFactory = (identifierField: IComplianceEntity['identifierField']): IComplianceEntity => ({
  identifierField,
  identifierType: null,
  logicalType: null,
  securityClassification: null,
  nonOwner: null,
  readonly: false,
  valuePattern: null
});

/**
 * Sorts a list of change set tuples by identifierField
 * @param {Array<IdentifierFieldWithFieldChangeSetTuple>} tuples
 * @return {Array<IdentifierFieldWithFieldChangeSetTuple>}
 */
const sortFoldedChangeSetTuples = (
  tuples: Array<IdentifierFieldWithFieldChangeSetTuple>
): Array<IdentifierFieldWithFieldChangeSetTuple> => {
  const tupleSortFn = (
    [fieldNameA]: IdentifierFieldWithFieldChangeSetTuple,
    [fieldNameB]: IdentifierFieldWithFieldChangeSetTuple
  ): number => fieldNameA.localeCompare(fieldNameB);

  return tuples.sort(tupleSortFn);
};

/**
 * Sets the readonly attribute on a tag to false
 * @param {IComplianceChangeSet} tag the readonly IComplianceChangeSet instance
 * @return {IComplianceChangeSet}
 */
const overrideTagReadonly = (tag: IComplianceChangeSet): IComplianceChangeSet =>
  setProperties(tag, { ...tag, readonly: false });

export {
  suggestedIdentifierTypesInList,
  compliancePolicyStrings,
  getFieldIdentifierOption,
  getFieldIdentifierOptions,
  complianceSteps,
  getComplianceSteps,
  editableTags,
  isAutoGeneratedPolicy,
  removeReadonlyAttr,
  tagNeedsReview,
  isTagIdType,
  mergeComplianceEntitiesWithSuggestions,
  isRecentSuggestion,
  tagsRequiringReview,
  tagsHaveNoneType,
  fieldTagsRequiringReview,
  tagsHaveNoneAndNotNoneType,
  initialComplianceObjectFactory,
  getIdTypeDataTypes,
  fieldTagsHaveIdentifierType,
  idTypeTagHasLogicalType,
  idTypeTagsHaveLogicalType,
  changeSetReviewableAttributeTriggers,
  asyncMapSchemaColumnPropsToCurrentPrivacyPolicy,
  foldComplianceChangeSets,
  complianceFieldChangeSetItemFactory,
  sortFoldedChangeSetTuples,
  tagsWithoutIdentifierType,
  tagsForIdentifierField,
  singleTagsInChangeSet,
  overrideTagReadonly
};
