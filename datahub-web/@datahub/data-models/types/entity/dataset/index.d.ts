import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { IEntityComplianceSuggestion } from '@datahub/metadata-types/constants/entity/dataset/compliance-suggestion';
import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';

/**
 * Describes the options for the dataset
 * @interface IReadDatasetsOptionBag
 */
export interface IReadDatasetsOptionBag {
  platform: DatasetPlatform | string;
  prefix: string;
  start?: number;
}

export interface IDatasetComplianceSuggestionInfo {
  lastModified: number;
  suggestedDatasetClassification: {};
  suggestedFieldClassification: Array<IEntityComplianceSuggestion>;
  urn: string;
}

/**
 * A map of DatasetComplianceAnnotation instances that are being added or removed from a dataset's compliance policy
 * Used to get a flat list of annotation instances, i.e. each separate annotation without field groupings
 * @export
 * @interface IComplianceAnnotationUpdates
 */
export interface IComplianceAnnotationUpdates {
  // Lists the annotations that are being added to the compliance policy
  added: Array<DatasetComplianceAnnotation>;
  // Lists the annotations that are  being removed from the compliance policy
  removed: Array<DatasetComplianceAnnotation>;
  // Lists the annotations that are mutations of previously saved annotations,
  // currently includes annotations found in added "and" removed since those prior lists are not pruned of intersections and technically,
  // a change is a removal and an addition
  onlyChanged: Array<DatasetComplianceAnnotation>;
}
