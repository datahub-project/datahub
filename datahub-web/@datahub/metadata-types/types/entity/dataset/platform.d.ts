import { PurgePolicy } from '@datahub/metadata-types/constants/entity/dataset/compliance/purge-policy';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

/**
 * Describes the interface for a platform object
 * @export
 * @interface IDataPlatform
 */
export interface IDataPlatform {
  // Policies supported for this dataset
  supportedPurgePolicies: Array<PurgePolicy>;
  // the type of the dataset platform with the given name e.g. DISTRIBUTED_FILE_SYSTEM, RELATIONAL_DB
  type: string;
  // the name of the dataset platform
  name: DatasetPlatform;
  // Not currently in use, but adding to sync our model for this interface with actual response format
  $URN: string;
  // The delimiter is used to separate dataset/entity paths for browsing and breadcrumbs.
  // Ex: dataset.path.here or dataset/path/here
  // Since the expected delimiter can vary by platform, this property is needed to calculate the
  // breadcrumbs correctly
  datasetNameDelimiter: string;
}

/**
 * Describes the interface for a response from a GET request to the list platforms endpoint
 * @export
 * @interface IPlatformsResponse
 */
export interface IPlatformsResponse {
  // optional platforms property containing the list of platforms
  platforms?: Array<IDataPlatform>;
}
