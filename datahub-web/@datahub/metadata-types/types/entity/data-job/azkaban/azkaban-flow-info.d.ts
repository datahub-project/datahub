import { IAzkabanProjectInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/azkaban-project-info';

/**
 * Metadata associated with a Azkaban flow
 * @export
 * @interface IAzkabanFlowInfo
 * @link https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/metadata-models/+/master/metadata-models/src/main/pegasus/com/linkedin/dataJob/azkaban/AzkabanFlowInfo.pdsc
 */
export interface IAzkabanFlowInfo {
  // Azkaban flow name
  flowName: string;
  // Azkaban project information including project name, version and cluster
  project: IAzkabanProjectInfo;
}
