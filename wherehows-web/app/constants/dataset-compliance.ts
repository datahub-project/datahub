import { PurgePolicy } from 'wherehows-web/constants/index';
import { IComplianceEntity, IComplianceInfo } from 'wherehows-web/typings/api/datasets/compliance';
import { IComplianceDataType } from 'wherehows-web/typings/api/list/compliance-datatypes';
import { arrayEvery, arrayFilter, arrayMap, arrayReduce } from 'wherehows-web/utils/array';
import { fleece, hasEnumerableKeys } from 'wherehows-web/utils/object';
import { lastSeenSuggestionInterval } from 'wherehows-web/constants/metadata-acquisition';
import { pick } from 'lodash';
import { decodeUrn } from 'wherehows-web/utils/validators/urn';
import {
  IComplianceChangeSet,
  IComplianceFieldIdentifierOption,
  IdentifierFieldWithFieldChangeSetTuple,
  IIdentifierFieldWithFieldChangeSetObject,
  ISchemaFieldsToPolicy,
  ISchemaFieldsToSuggested
} from 'wherehows-web/typings/app/dataset-compliance';
import {
  IColumnFieldProps,
  ICompliancePolicyReducerFactory,
  ISchemaColumnMappingProps
} from 'wherehows-web/typings/app/dataset-columns';

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
  successUploading: 'Metadata successfully updated! Please "Save" when ready.',
  invalidPolicyData: 'Received policy in an unexpected format! Please check the provided attributes and try again.',
  helpText: {
    classification:
      'This security classification is from go/dht and should be good enough in most cases. ' +
      'You can optionally override it if required by house security.'
  },
  missingPurgePolicy: 'Please specify a Compliance Purge Policy',
  missingDatasetSecurityClassification: 'Please specify a security classification for this dataset.'
};

/**
 * Field / changeSet attributes that will trigger a check if review is requested
 * field `logicalType` in `changeSetReviewableAttributeTriggers` is used in the determination of idType fields
 * without a logicalType as requiring review
 * @type {string}
 */
const changeSetReviewableAttributeTriggers = 'isDirty,suggestion,privacyPolicyExists,suggestionAuthority,logicalType';

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
const isEditableComplianceEntity = ({ readonly }: IComplianceEntity): boolean => readonly !== true;

/**
 * Filters out from a list of compliance entities, entities that are editable
 * @param {Array<IComplianceEntity>} entities
 * @returns {Array<IComplianceEntity>}
 */
const filterEditableEntities = (entities: Array<IComplianceEntity>): Array<IComplianceEntity> =>
  arrayFilter(isEditableComplianceEntity)(entities);

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
 * Checks if a compliance policy changeSet field requires user attention: if a suggestion
 * is available  but the user has not indicated intent or a policy for the field does not currently exist remotely
 * and the related field changeSet has not been modified on the client and isn't readonly
 * @param {boolean} isDirty
 * @return {boolean}
 */
/**
 *
 * @param {Array<IComplianceDataType>} complianceDataTypes
 * @return {(changeSet: IComplianceChangeSet) => boolean}
 */
const fieldChangeSetRequiresReview = (complianceDataTypes: Array<IComplianceDataType>) =>
  /**
   *
   * @param {IComplianceChangeSet} changeSet
   * @return {boolean}
   */
  (changeSet: IComplianceChangeSet): boolean => {
    const { isDirty, suggestion, privacyPolicyExists, suggestionAuthority, readonly } = changeSet;
    let isReviewRequired = false;

    if (readonly) {
      return false;
    }

    if (suggestion) {
      isReviewRequired = isReviewRequired || !suggestionAuthority;
    }

    if (isFieldIdType(complianceDataTypes)(changeSet)) {
      isReviewRequired = isReviewRequired || !idTypeFieldHasLogicalType(changeSet);
    }
    // If either the privacy policy doesn't exists, or user hasn't made changes, then review is required
    return isReviewRequired || !(privacyPolicyExists || isDirty);
  };

const isFieldIdType = (complianceDataTypes: Array<IComplianceDataType> = []) => ({
  identifierType
}: IComplianceChangeSet): boolean => getIdTypeDataTypes(complianceDataTypes).includes(<string>identifierType);

const idTypeFieldHasLogicalType = ({ logicalType }: IComplianceEntity): boolean => !!logicalType;

const idTypeFieldsHaveLogicalType = arrayEvery(idTypeFieldHasLogicalType);
/**
 * Gets the fields requiring review
 * @type {(array: Array<IComplianceChangeSet>) => Array<boolean>}
 */
const getFieldsRequiringReview = (complianceDataTypes: Array<IComplianceDataType>) =>
  arrayMap(fieldChangeSetRequiresReview(complianceDataTypes));

/**
 * Returns a list of changeSet fields that requires user attention
 * @type {function({}): Array<{ isDirty, suggestion, privacyPolicyExists, suggestionAuthority }>}
 */
const changeSetFieldsRequiringReview = (complianceDataTypes: Array<IComplianceDataType>) =>
  arrayFilter<IComplianceChangeSet>(fieldChangeSetRequiresReview(complianceDataTypes));

/**
 * Merges the column fields with the suggestion for the field if available
 * @param {object} mappedColumnFields a map of column fields to compliance entity properties
 * @param {object} fieldSuggestionMap a map of field suggestion properties keyed by field name
 * @return {Array<object>} mapped column field augmented with suggestion if available
 */
const mergeMappedColumnFieldsWithSuggestions = (
  mappedColumnFields: ISchemaFieldsToPolicy = {},
  fieldSuggestionMap: ISchemaFieldsToSuggested = {}
): Array<IComplianceChangeSet> =>
  Object.keys(mappedColumnFields).map(fieldName => {
    const field = pick(mappedColumnFields[fieldName], [
      'identifierField',
      'dataType',
      'identifierType',
      'logicalType',
      'securityClassification',
      'policyModificationTime',
      'privacyPolicyExists',
      'isDirty',
      'nonOwner',
      'readonly'
    ]);
    const { identifierField, policyModificationTime } = field;
    const suggestion = fieldSuggestionMap[identifierField];

    // If a suggestion exists for this field add the suggestion attribute to the field properties / changeSet
    // Check if suggestion isRecent before augmenting, otherwise, suggestion will not be considered on changeSet
    if (suggestion && isRecentSuggestion(policyModificationTime, suggestion.suggestionsModificationTime)) {
      return { ...field, suggestion };
    }

    return field;
  });

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
  [changeSet.identifierField]: [...identifierFieldMap[changeSet.identifierField], changeSet]
});

/**
 * Reduces a list of IComplianceChangeSet to a list of tuples with a complianceChangeSet identifierField
 * and a changeSet list
 * @param {Array<IComplianceChangeSet>} changeSet
 * @returns {Array<IdentifierFieldWithFieldChangeSetTuple>}
 */
const foldComplianceChangeSets = (
  changeSet: Array<IComplianceChangeSet>
): Array<IdentifierFieldWithFieldChangeSetTuple> =>
  Object.entries<Array<IComplianceChangeSet>>(arrayReduce(foldComplianceChangeSetToField, {})(changeSet));

/**
 * Builds a default shape for securitySpecification & privacyCompliancePolicy with default / unset values
 *   for non null properties as per Avro schema
 * @param {string} datasetId identifier for the dataset that this privacy object applies to
 */
const createInitialComplianceInfo = (datasetId: string): IComplianceInfo => {
  const identifier = typeof datasetId === 'string' ? { datasetUrn: decodeUrn(datasetId) } : { datasetId };

  return {
    ...identifier,
    datasetId: null,
    confidentiality: null,
    complianceType: '',
    compliancePurgeNote: '',
    complianceEntities: [],
    datasetClassification: null
  };
};

/**
 * Extracts the values on a compliance Entity for a given list of keys
 * @template K IComplianceEntity instance attribute
 * @param {Array<K>} [keys=[]]
 * @param {string} fieldName
 * @param {IComplianceInfo.complianceEntities} [source=[]]
 * @returns {({ [V in K]: IComplianceEntity[V] } | {})}
 */
const getKeysOnComplianceEntity = <K extends keyof IComplianceEntity>(
  keys: Array<K> = [],
  fieldName: string,
  source: IComplianceInfo['complianceEntities'] = []
): { [V in K]: IComplianceEntity[V] } | {} => {
  const sourceField: IComplianceEntity | void = source.find(({ identifierField }) => identifierField === fieldName);
  let result = {};

  if (sourceField) {
    for (const [key, value] of <Array<[K, IComplianceEntity[K]]>>Object.entries(sourceField)) {
      if (keys.includes(key)) {
        result = { ...result, [key]: value };
      }
    }
  }

  return result;
};

/**
 * Maps the fields found in the column property on the schema api to the values returned in the current privacy policy
 * @param {ISchemaColumnMappingProps} {
 *   columnProps,
 *   complianceEntities,
 *   policyModificationTime
 * }
 * @returns {ISchemaFieldsToPolicy}
 */
const mapSchemaColumnPropsToCurrentPrivacyPolicy = ({
  columnProps,
  complianceEntities,
  policyModificationTime
}: ISchemaColumnMappingProps): ISchemaFieldsToPolicy =>
  arrayReduce(columnToPolicyReducingFn(complianceEntities, policyModificationTime), {})(columnProps);

/**
 * Takes the current compliance entities, and mod time and returns a reducer that consumes a list of IColumnFieldProps
 * instances and maps each entry to a compliance entity on the current compliance policy
 * @param {IComplianceInfo.complianceEntities} currentEntities
 * @param {IComplianceInfo.modifiedTime} policyModificationTime
 * @type ICompliancePolicyReducerFactory
 */
const columnToPolicyReducingFn: ICompliancePolicyReducerFactory = (
  currentEntities: IComplianceInfo['complianceEntities'],
  policyModificationTime: IComplianceInfo['modifiedTime']
) => (acc: ISchemaFieldsToPolicy, { identifierField, dataType }: IColumnFieldProps) => {
  const currentPrivacyAttrs = getKeysOnComplianceEntity(
    ['identifierType', 'logicalType', 'securityClassification', 'nonOwner', 'readonly'],
    identifierField,
    currentEntities
  );

  // assertion required due to TS spread object limitation, not present with Object#assign, but this reads cleaner
  return <ISchemaFieldsToPolicy>{
    ...acc,
    [identifierField]: {
      identifierField,
      dataType,
      readonly: false, // default value overridden by value in currentPrivacyAttrs below
      ...currentPrivacyAttrs,
      policyModificationTime,
      privacyPolicyExists: hasEnumerableKeys(currentPrivacyAttrs),
      isDirty: false
    }
  };
};

export {
  compliancePolicyStrings,
  getFieldIdentifierOption,
  getFieldIdentifierOptions,
  complianceSteps,
  getComplianceSteps,
  filterEditableEntities,
  isAutoGeneratedPolicy,
  removeReadonlyAttr,
  fieldChangeSetRequiresReview,
  isFieldIdType,
  mergeMappedColumnFieldsWithSuggestions,
  isRecentSuggestion,
  getFieldsRequiringReview,
  createInitialComplianceInfo,
  getIdTypeDataTypes,
  idTypeFieldHasLogicalType,
  idTypeFieldsHaveLogicalType,
  changeSetFieldsRequiringReview,
  changeSetReviewableAttributeTriggers,
  mapSchemaColumnPropsToCurrentPrivacyPolicy,
  foldComplianceChangeSets
};
