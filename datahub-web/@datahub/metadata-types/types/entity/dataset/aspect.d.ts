import { IBaseAspect } from '@datahub/metadata-types/types/metadata/aspect';

/**
 * A specific metadata aspect for a dataset
 * @export
 * @namespace metadata.aspect
 * @interface IDatasetAspect
 */
export interface IDatasetAspect extends IBaseAspect {
  // URN for the dataset the metadata aspect is associated with
  urn: string;
  // The specific metadata aspect
  'com.linkedin.dataset.ump.UMPDatasetProperties'?: Com.Linkedin.Dataset.Ump.UMPDatasetProperties;
  'com.linkedin.dataset.RetentionPolicy'?: Com.Linkedin.Dataset.RetentionPolicy;
  'com.linkedin.dataset.ComplianceInfo'?: Com.Linkedin.Dataset.ComplianceInfo;
  'com.linkedin.common.Ownership'?: Com.Linkedin.Common.Ownership;
}
