import { PurgePolicy } from '@datahub/metadata-types/constants/entity/dataset/compliance/purge-policy';

/**
 * A dataset retention policy determines how long should the dataset be kept before it should be purged
 */
export interface IDatasetRetentionPolicy {
  // Unique wherehows specific database identifier, at this point is legacy but is still being returned by API
  datasetId: null;
  // Unique urn for the dataset that helps identify it
  datasetUrn: string;
  // optional string with username of modifier
  modifiedBy: string;
  // optional timestamp of last modification date
  modifiedTime: number;
  // User entered purge notation for a dataset with a purge exempt policy
  purgeNote: string | null;
  // Purge Policy for the dataset
  purgeType: PurgePolicy | '';
}
