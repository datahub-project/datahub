import { IComplianceInfo } from 'datahub-web/typings/api/datasets/compliance';

/**
 * Describes the interface for a dataset retention policy
 *
 * @interface IDatasetRetention
 */
export interface IDatasetRetention {
  // Unused attribute for the id of the dataset, use datasetUrn instead
  datasetId?: number | null;
  // Urn string identifying the dataset that owns this policy
  datasetUrn: string;
  // Purge Policy for the dataset
  purgeType: IComplianceInfo['complianceType'];
  // User entered purge notation for a dataset with a purge exempt policy
  purgeNote: IComplianceInfo['compliancePurgeNote'];
  // Who modified this retention policy
  modifiedBy?: string;
  // When this policy was last modified
  modifiedTime?: number;
}

/**
 * Describes the return type for requests to the endpoint returning the dataset retention policy
 * @interface IGetDatasetRetentionResponse
 */
export interface IGetDatasetRetentionResponse {
  retentionPolicy: IDatasetRetention;
}
