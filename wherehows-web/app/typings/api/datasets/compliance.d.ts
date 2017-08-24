import { ApiStatus } from 'wherehows-web/utils/api/shared';

/**
 * Describes the prediction property in a compliance suggestion
 */
interface IPrediction {
  // Percentage confidence level for this prediction
  confidence: number;
  // The predicted value
  value: string;
}

/**
 * Describes shape of a compliance auto suggestion
 */
export interface IComplianceSuggestion {
  // The name of the field
  fieldName: string;
  // Prediction for the identifierType
  identifierTypePrediction: IPrediction | null;
  // Prediction for the logicalType
  logicalTypePrediction: IPrediction | null;
}

/**
 * Describes the expected properties for the autoClassification on a compliance suggestions api response
 */
interface IAutoClassification {
  // The urn for the dataset
  urn: string;
  // a JSON string: array of suggestions available for  fields on this dataset
  classificationResult: string;
  // the last modified date for the suggestion
  lastModified: number;
}

/**
 * Describes the expected affirmative API response for a the compliance suggestion
 */
export interface IComplianceSuggestionResponse {
  status: ApiStatus;
  autoClassification?: IAutoClassification;
}
