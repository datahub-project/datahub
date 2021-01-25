import {
  IComplianceEntity,
  IComplianceInfo,
  ISuggestedFieldClassification
} from 'datahub-web/typings/api/datasets/compliance';
import {
  NonMemberIdLogicalType,
  MemberIdLogicalType,
  Classification,
  ComplianceFieldIdValue
} from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';
import { SuggestionIntent } from '@datahub/data-models/constants/entity/dataset/compliance-suggestions';
import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';

/**
 * Describes the DatasetCompliance actions index signature to allow
 * access to actions using `did${editStepName}` accessors
 */
export interface IDatasetComplianceActions {
  didEditCompliancePolicy: () => Promise<void>;
  didEditPurgePolicy: () => Promise<void>;
  didEditDatasetLevelCompliancePolicy: () => Promise<void>;
  [K: string]: (...args: Array<unknown>) => unknown;
}

/**
 * Describes the interface for compliance tag review check function, options argument
 * @interface IComplianceTagReviewOptions
 */
export interface IComplianceTagReviewOptions {
  // flag determines if suggested values are considered in tag(IComplianceChangeSet) review check
  checkSuggestions: boolean;
  // confidence threshold for filtering out higher quality suggestions
  suggestionConfidenceThreshold: number;
}

/**
 * Alias for the properties defined on an object indicating the values for a compliance entity object in
 * addition to related component metadata using in processing ui interactions / rendering for the field
 */
export type IComplianceEntityWithMetadata = Pick<
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
export interface ISchemaFieldsToPolicy {
  [fieldName: string]: Array<IComplianceEntityWithMetadata>;
}

/**
 * Alias for the properties on an object indicating the suggested values for field / record properties
 * as well as suggestions metadata
 */
export type SchemaFieldToSuggestedValue = Pick<
  IComplianceEntity,
  'identifierType' | 'logicalType' | 'securityClassification'
> &
  Pick<ISuggestedFieldClassification, 'confidenceLevel' | 'uid'>;

/**
 * Describes the mapping of attributes to value types for a datasets schema field names to suggested property values
 * @interface ISchemaFieldsToSuggested
 */
export interface ISchemaFieldsToSuggested {
  [fieldName: string]: SchemaFieldToSuggestedValue;
}
/**
 * Describes the interface for a locally assembled compliance field instance
 * used in rendering a compliance row
 */
export type IComplianceChangeSet = {
  suggestion?: SchemaFieldToSuggestedValue;
  suggestionAuthority?: SuggestionIntent;
} & IComplianceEntityWithMetadata;

/**
 * Describes the mapping of an identifier field to it's compliance changeset list
 * @interface IIdentifierFieldWithFieldChangeSetObject
 */
export interface IIdentifierFieldWithFieldChangeSetObject {
  [identifierField: string]: Array<IComplianceChangeSet>;
}

/**
 * Defines a type for identifierField with it's changeSet tuple
 */
export type IdentifierFieldWithFieldChangeSetTuple = [string, Array<IComplianceChangeSet>];

/**
 * Defines the interface for compliance suggestion values extracted from ISuggestedFieldClassification
 * @interface ISuggestedFieldTypeValues
 */
export interface ISuggestedFieldTypeValues {
  identifierType: IComplianceChangeSet['identifierType'];
  logicalType: IComplianceChangeSet['logicalType'];
  confidence: number;
}

/**
 * Defines the interface for compliance data type field option
 * @interface IComplianceFieldIdentifierOption
 */
export interface IComplianceFieldIdentifierOption
  extends INachoDropdownOption<ComplianceFieldIdValue | NonMemberIdLogicalType> {
  isId: boolean;
}

/**
 * Defines the interface for a compliance field format dropdown option
 * @alias IComplianceFieldFormatOption
 * @extends {(INachoDropdownOption<IdLogicalType | null>)}
 */
export type IComplianceFieldFormatOption = INachoDropdownOption<MemberIdLogicalType | null>;

/**
 * Defines the interface for an each security classification dropdown option
 * @alias ISecurityClassificationOption
 * @extends {(INachoDropdownOption<Classification | null>)}
 */
export type ISecurityClassificationOption = INachoDropdownOption<Classification | null>;
