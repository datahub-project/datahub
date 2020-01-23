import { IBaseAspect } from '@datahub/metadata-types/types/metadata/aspect';
import { IOwnership } from '@datahub/metadata-types/types/common/ownership';

/**
 * A specific metadata aspect for a dataset
 * @export
 * @namespace metadata.aspect
 * @interface IDatasetAspect
 */
export interface IDatasetAspect extends IBaseAspect {
  // URN for the dataset the metadata aspect is associated with
  urn: string;
  'com.linkedin.common.Ownership'?: IOwnership;
}
