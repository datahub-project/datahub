import { ApiStatus } from 'wherehows-web/utils/api/shared';
import {
  FieldIdValues,
  Classification,
  IdLogicalType,
  CustomIdLogicalType,
  NonIdLogicalType,
  DatasetClassifiers
} from 'wherehows-web/constants';

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
    identifierType: FieldIdValues;
    identifierField: string;
    logicalType: IdLogicalType | CustomIdLogicalType | NonIdLogicalType;
    securityClassification: Classification;
  };
}

/**
 * Describes the interface for an object containing suggested compliance metadata for a dataset
 * @export
 * @interface ISuggestedDatasetClassification
 */
export type ISuggestedDatasetClassification = {
  [K in DatasetClassifiers]: {
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
