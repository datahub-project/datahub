import {
  IComplianceEntity,
  IComplianceInfo,
  ISuggestedFieldClassification,
  IComplianceSuggestion
} from 'wherehows-web/typings/api/datasets/compliance';
import { NonIdLogicalType, SuggestionIntent } from 'wherehows-web/constants';
import { Classification, ComplianceFieldIdValue, IdLogicalType } from 'wherehows-web/constants/datasets/compliance';

/**
 * Describes the DatasetCompliance actions index signature to allow
 * access to actions using `did${editStepName}` accessors
 */
interface IDatasetComplianceActions {
  didEditCompliancePolicy: () => Promise<void>;
  didEditPurgePolicy: () => Promise<void>;
  didEditDatasetLevelCompliancePolicy: () => Promise<void>;
  [K: string]: (...args: Array<any>) => any;
}

/**
 * Describes the interface for compliance tag review check function, options argument
 * @interface IComplianceTagReviewOptions
 */
interface IComplianceTagReviewOptions {
  // flag determines if suggested values are considered in tag(IComplianceChangeSet) review check
  checkSuggestions: boolean;
  // confidence threshold for filtering out higher quality suggestions
  suggestionConfidenceThreshold: number;
}

/**
 * Alias for the properties defined on an object indicating the values for a compliance entity object in
 * addition to related component metadata using in processing ui interactions / rendering for the field
 */
type IComplianceEntityWithMetadata = Pick<
  IComplianceEntity,
  | 'identifierField'
  | 'identifierType'
  | 'logicalType'
  | 'securityClassification'
  | 'nonOwner'
  | 'readonly'
  | 'valuePattern'
  | 'pii'
> & {
  // flag indicating that the field has a current policy upstream
  privacyPolicyExists: boolean;
  // flag indicating the field changeSet has been modified on the client
  isDirty: boolean;
  policyModificationTime?: IComplianceInfo['modifiedTime'];
  dataType: string;
};

/**
 * Describes the interface for a mapping of field names to type, IComplianceEntityWithMetadata
 * @interface ISchemaFieldsToPolicy
 */
interface ISchemaFieldsToPolicy {
  [fieldName: string]: Array<IComplianceEntityWithMetadata>;
}

/**
 * Alias for the properties on an object indicating the suggested values for field / record properties
 * as well as suggestions metadata
 */
type SchemaFieldToSuggestedValue = Pick<
  IComplianceEntity,
  'identifierType' | 'logicalType' | 'securityClassification'
> &
  Pick<ISuggestedFieldClassification, 'confidenceLevel' | 'uid'> & {
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
type IComplianceChangeSet = {
  suggestion?: SchemaFieldToSuggestedValue;
  suggestionAuthority?: SuggestionIntent;
} & IComplianceEntityWithMetadata;

/**
 * Describes the mapping of an identifier field to it's compliance changeset list
 * @interface IIdentifierFieldWithFieldChangeSetObject
 */
interface IIdentifierFieldWithFieldChangeSetObject {
  [identifierField: string]: Array<IComplianceChangeSet>;
}

/**
 * Defines a type for identifierField with it's changeSet tuple
 */
type IdentifierFieldWithFieldChangeSetTuple = [string, Array<IComplianceChangeSet>];

/**
 * Defines the interface for compliance suggestion values extracted from ISuggestedFieldClassification
 * @interface ISuggestedFieldTypeValues
 */
interface ISuggestedFieldTypeValues {
  identifierType: IComplianceChangeSet['identifierType'];
  logicalType: IComplianceChangeSet['logicalType'];
  confidence: number;
}

/**
 * Defines the generic interface field identifier drop downs
 * @interface IDropDownOption
 * @template T
 */
interface IDropDownOption<T> {
  value: T;
  label: string;
  isDisabled?: boolean;
}

/**
 * Defines the interface for compliance data type field option
 * @interface IComplianceFieldIdentifierOption
 * @extends {IDropDownOption<ComplianceFieldIdValue>}
 */
interface IComplianceFieldIdentifierOption extends IDropDownOption<ComplianceFieldIdValue | NonIdLogicalType> {
  isId: boolean;
}

/**
 * Defines the interface for a compliance field format dropdown option
 * @alias IComplianceFieldFormatOption
 * @extends {(IDropDownOption<IdLogicalType | null>)}
 */
type IComplianceFieldFormatOption = IDropDownOption<IdLogicalType | null>;

/**
 * Defines the interface for an each security classification dropdown option
 * @alias ISecurityClassificationOption
 * @extends {(IDropDownOption<Classification | null>)}
 */
type ISecurityClassificationOption = IDropDownOption<Classification | null>;

/**
 * Defines the applicable string values for compliance fields drop down filter
 */
type ShowAllShowReview = 'showReview' | 'showAll';

export {
  IComplianceChangeSet,
  ShowAllShowReview,
  IDatasetComplianceActions,
  IComplianceEntityWithMetadata,
  ISchemaFieldsToPolicy,
  SchemaFieldToSuggestedValue,
  ISchemaFieldsToSuggested,
  IDropDownOption,
  IComplianceFieldIdentifierOption,
  IComplianceFieldFormatOption,
  ISecurityClassificationOption,
  IIdentifierFieldWithFieldChangeSetObject,
  IdentifierFieldWithFieldChangeSetTuple,
  ISuggestedFieldTypeValues,
  IComplianceTagReviewOptions
};
