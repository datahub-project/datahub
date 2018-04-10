import {
  IComplianceEntity,
  IComplianceInfo,
  ISuggestedFieldClassification,
  IComplianceSuggestion
} from 'wherehows-web/typings/api/datasets/compliance';
import { SuggestionIntent } from 'wherehows-web/constants';
import { Classification, ComplianceFieldIdValue, IdLogicalType } from 'wherehows-web/constants/datasets/compliance';

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
  'identifierField' | 'identifierType' | 'logicalType' | 'securityClassification' | 'nonOwner' | 'readonly'
> & {
  // flag indicating that the field has a current policy upstream
  privacyPolicyExists: boolean;
  // flag indicating the field changeSet has been modified on the client
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
} & SchemaFieldToPolicyValue;

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
interface IComplianceFieldIdentifierOption extends IDropDownOption<ComplianceFieldIdValue> {
  isId: boolean;
}

/**
 * Defines the interface for a compliance field format dropdown option
 * @interface IComplianceFieldFormatOption
 * @extends {(IDropDownOption<IdLogicalType | null>)}
 */
interface IComplianceFieldFormatOption extends IDropDownOption<IdLogicalType | null> {}

/**
 * Defines the interface for an each security classification dropdown option
 * @interface ISecurityClassificationOption
 * @extends {(IDropDownOption<Classification | null>)}
 */
interface ISecurityClassificationOption extends IDropDownOption<Classification | null> {}

/**
 * Defines the applicable string values for compliance fields drop down filter
 */
type ShowAllShowReview = 'showReview' | 'showAll';

export {
  IComplianceChangeSet,
  ShowAllShowReview,
  IDatasetComplianceActions,
  SchemaFieldToPolicyValue,
  ISchemaFieldsToPolicy,
  SchemaFieldToSuggestedValue,
  ISchemaFieldsToSuggested,
  IDropDownOption,
  IComplianceFieldIdentifierOption,
  IComplianceFieldFormatOption,
  ISecurityClassificationOption
};
