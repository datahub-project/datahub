import { ApiStatus } from '@datahub/utils/api/shared';
import { DatasetClassifiers, DatasetClassification } from 'datahub-web/constants';
import {
  MemberIdLogicalType,
  NonMemberIdLogicalType,
  ComplianceFieldIdValue,
  Classification
} from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';
import { PurgePolicy } from '@datahub/metadata-types/constants/entity/dataset/compliance/purge-policy';

/**
 * Describes the interface for a compliance entity object.
 * This specifies the compliance properties for field in a dataset schema.
 * Typically returned in the list of compliance entities field in the IComplianceGetResponse
 * @export
 * @interface IComplianceEntity
 */
export interface IComplianceEntity {
  // Unique identifier for a complianceEntity, must match a field with equal field path in the dataset schema
  identifierField: string;
  // Tag indicating the type of the the field
  identifierType: ComplianceFieldIdValue | NonMemberIdLogicalType | null;
  // Tag indicating the type of the identifierType,
  // solely applicable to corresponding id's with an idType of `false` in the complianceDataTypes endpoint
  logicalType: MemberIdLogicalType | null;
  // User specified / default security classification for the related schema field
  securityClassification: Classification | null;
  // Flag indicating that the dataset is of a subject type, default is false
  nonOwner: boolean | null;
  // Flag indicating that this compliance field is not editable by the end user
  // field should also be filtered from persisted policy
  readonly readonly?: boolean;
  // Optional attribute for the value of a CUSTOM regex. Required for CUSTOM field format
  valuePattern?: string | null;
  // Flags this entity as containing pii data
  pii?: boolean;
}

/**
 * Describes the interface for a dataset's complianceInfo, which represents the
 * privacy and/or compliance information for that dataset
 * @export
 * @interface IComplianceInfo
 */
export interface IComplianceInfo {
  // List of compliance attributes for fields on the dataset
  complianceEntities: Array<IComplianceEntity>;
  // User entered purge notation for a dataset with a purge exempt policy
  compliancePurgeNote: null | string;
  // Purge Policy for the dataset
  complianceType: PurgePolicy | '';
  // Dataset level security classification
  confidentiality: Classification | null;
  // Flag indicating that the dataset contains pii data, typically for schemaless dataset this is user entered,
  // for datasets with a schema, this derived from the complianceEntities
  containingPersonalData?: boolean | null;
  // Tags for a types of data contained in the related dataset
  datasetClassification: DatasetClassification | null;
  // Unique wherehows specific database identifier
  datasetId: null;
  // Unique urn for the dataset
  readonly datasetUrn?: string;
  // optional string with username of modifier
  modifiedBy?: string;
  // optional timestamp of last modification date
  modifiedTime?: string;
  // optional attribute indicating that the compliance policy is derived from a parent in the lineage
  readonly fromUpstream?: boolean;
}

/**
 * Describes the response from the GET dataset compliance endpoint
 * @export
 * @interface IComplianceGetResponse
 */
export interface IComplianceGetResponse {
  complianceInfo?: IComplianceInfo;
  msg?: string;
}

/**
 * Describes the interface for a dataset's compliance suggestion
 * @export
 * @interface IComplianceSuggestion
 */
export interface IComplianceSuggestion {
  // The urn for the dataset
  urn: string;
  // A list of suggested values for each field
  suggestedFieldClassification: Array<ISuggestedFieldClassification>;
  // A key value pair for dataset classification keys to suggested boolean values
  suggestedDatasetClassification: ISuggestedDatasetClassification;
  // the last modified date for the suggestion
  lastModified: number;
}

/**
 * Describes the interface for an object containing suggested compliance metadata field values
 * @export
 * @interface ISuggestedFieldClassification
 */
export interface ISuggestedFieldClassification {
  // uuid for the field suggestion
  uid?: string;
  confidenceLevel: number;
  suggestion: {
    identifierType: IComplianceEntity['identifierType'];
    identifierField: IComplianceEntity['identifierField'];
    logicalType: IComplianceEntity['logicalType'];
    securityClassification: IComplianceEntity['securityClassification'];
    nonOwner: IComplianceEntity['nonOwner'];
  };
}

/**
 * Describes the interface for an object containing suggested compliance metadata for a dataset
 * @export
 * @interface ISuggestedDatasetClassification
 */
export type ISuggestedDatasetClassification = {
  [K in keyof typeof DatasetClassifiers]: {
    contain: boolean;
    confidenceLevel: number;
  };
};

/**
 * Describes the expected affirmative API response for a the compliance suggestion
 * @export
 * @interface IComplianceSuggestionResponse
 */
export interface IComplianceSuggestionResponse {
  status: ApiStatus;
  complianceSuggestion?: IComplianceSuggestion;
}

/**
 * Describes the interface for the dataset export policy fields
 * @export
 * @interface IDatasetExportPolicy
 */
export interface IDatasetExportPolicy {
  containsUserGeneratedContent: boolean;
  containsUserActionGeneratedContent: boolean;
  containsUserDerivedContent: boolean;
  modifiedBy?: string;
  modifiedTime?: number;
}

/**
 * Describes the expected affirmative API response for the dataset export policy
 * @export
 * @interface IDatasetExportPolicyResponse
 */
export interface IDatasetExportPolicyResponse {
  exportPolicy: IDatasetExportPolicy;
  status: ApiStatus;
}
