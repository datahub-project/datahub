import { ApiStatus } from 'wherehows-web/utils/api/shared';
import {
  ComplianceFieldIdValue,
  Classification,
  IdLogicalType,
  NonIdLogicalType,
  DatasetClassifiers,
  PurgePolicy,
  DatasetClassification
} from 'wherehows-web/constants';

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
  identifierType: ComplianceFieldIdValue | NonIdLogicalType | null;
  // Tag indicating the type of the identifierType,
  // solely applicable to corresponding id's with an idType of `false` in the complianceDataTypes endpoint
  logicalType: IdLogicalType | null;
  // User specified / default security classification for the related schema field
  securityClassification: Classification | null;
  // Flag indicating that the dataset is of a subject type, default is false
  nonOwner: boolean;
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
  complianceType: PurgePolicy;
  // Dataset level security classification
  confidentiality: Classification | null;
  // Flag indicating that the dataset contains pii data, typically for schemaless dataset this is user entered,
  // for datasets with a schema, this derived from the complianceEntities
  containingPersonalData?: boolean;
  // Tags for a types of data contained in the related dataset
  datasetClassification: DatasetClassification;
  // Unique wherehows specific database identifier
  datasetId: number;
  // Unique urn for the dataset
  datasetUrn?: string;
  // optional string with username of modifier
  modifiedBy?: string;
  // optional timestamp of last modification date
  modifiedTime?: string;
}

/**
 * Describes the response from the GET dataset compliance endpoint
 * @export
 * @interface IComplianceGetResponse
 */
export interface IComplianceGetResponse {
  status: ApiStatus;
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
  }
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
