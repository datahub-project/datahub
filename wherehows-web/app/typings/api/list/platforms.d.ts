import { DatasetPlatform, PurgePolicy } from 'wherehows-web/constants';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

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
}

/**
 * Describes the interface for a response from a GET request to the list platforms endpoint
 * @export
 * @interface IPlatformsResponse
 */
export interface IPlatformsResponse {
  // Response status
  status: ApiStatus;
  // optional platforms property containing the list of platforms
  platforms?: Array<IDataPlatform>;
  // optional api error message
  msg?: string;
}
