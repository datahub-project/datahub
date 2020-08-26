import { IBaseAspect } from '@datahub/metadata-types/types/metadata/aspect';
import { IAzkabanJobInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/job/azkaban-job-info';
import { AzkabanJobInput } from '@datahub/metadata-types/types/entity/data-job/azkaban/job/azkaban-job-input';
import { AzkabanJobOutput } from '@datahub/metadata-types/types/entity/data-job/azkaban/job/azkaban-job-output';

/**
 * A union of all supported metadata aspects for a Azkaban job
 * @export
 * @interface IAzkabanJobAspect
 * @extends {IBaseAspect}
 * @link https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/metadata-models/+/master/metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/AzkabanJobAspect.pdsc
 */
export interface IAzkabanJobAspect extends IBaseAspect {
  // URN for the associated Azkaban job
  urn: string;
  // Ownership information of an entity
  'com.linkedin.common.Ownership'?: Com.Linkedin.Common.Ownership;
  // Metadata associated with a Azkaban job
  'com.linkedin.dataJob.azkaban.AzkabanJobInfo'?: IAzkabanJobInfo;
  // Inputs consumed by the Azkaban job
  'com.linkedin.dataJob.azkaban.AzkabanJobInput'?: AzkabanJobInput;
  // Outputs produced by the Azkaban job
  'com.linkedin.dataJob.azkaban.AzkabanJobOutput'?: AzkabanJobOutput;
}
