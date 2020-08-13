import { IComplianceFieldAnnotation } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-annotation';

/**
 * Raw data format of a compliance suggestion for an entity, in this case a dataset
 * @export
 * @namespace Dataset
 */
export interface IEntityComplianceSuggestion {
  confidenceLevel: number;
  suggestion: IComplianceFieldAnnotation;
  uid: string;
}

/**
 * Possible sources for a suggestion to originate from. Currently we only support and worry
 * about system suggestions
 * @export
 * @namespace Dataset
 * @enum {string}
 */
export enum SuggestionSource {
  system = 'system'
}
