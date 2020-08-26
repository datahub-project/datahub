import { IAzkabanFlowInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/azkaban-flow-info';

/**
 * Metadata associated with a Azkaban job
 * @export
 * @interface IAzkabanJobInfo
 * @link https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/metadata-models/+/master/metadata-models/src/main/pegasus/com/linkedin/dataJob/azkaban/AzkabanJobInfo.pdsc
 */
export interface IAzkabanJobInfo extends IAzkabanFlowInfo {
  // Azkaban job name
  jobName: string;
}
