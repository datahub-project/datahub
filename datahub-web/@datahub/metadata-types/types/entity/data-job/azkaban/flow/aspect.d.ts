import { IBaseAspect } from '@datahub/metadata-types/types/metadata/aspect';
import { IAzkabanFlowInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/azkaban-flow-info';
import { IAzkabanFlowJobsInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/flow/azkaban-flow-jobs-info';

/**
 * A union of all supported metadata aspects for a Azkaban flow
 * @export
 * @interface IAzkabanFlowAspect
 * @extends {IBaseAspect}
 * @link https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/metadata-models/+/master/metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/AzkabanFlowAspect.pdsc
 */
export interface IAzkabanFlowAspect extends IBaseAspect {
  // Ownership information of an entity
  'com.linkedin.common.Ownership'?: Com.Linkedin.Common.Ownership;
  // Metadata associated with a Azkaban job
  'com.linkedin.dataJob.azkaban.AzkabanFlowInfo'?: IAzkabanFlowInfo;
  // Inputs consumed by the Azkaban job
  'com.linkedin.dataJob.azkaban.AzkabanFlowJobsInfo'?: IAzkabanFlowJobsInfo;
}
