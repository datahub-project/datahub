import { IAzkabanClusterInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/azkaban-cluster-info';

/**
 * Metadata associated with a Azkaban project
 * @export
 * @interface IAzkabanProjectInfo
 * @link https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/metadata-models/+/master/metadata-models/src/main/pegasus/com/linkedin/dataJob/azkaban/AzkabanProjectInfo.pdsc
 */
export interface IAzkabanProjectInfo {
  // Azkaban project name
  projectName: string;
  // Azkaban project upload version
  projectVersion: string;
  // Hadoop cluster in which the project exists
  clusterInfo: IAzkabanClusterInfo;
}
