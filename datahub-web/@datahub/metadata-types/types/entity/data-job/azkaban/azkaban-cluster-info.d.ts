import { HadoopClusterType } from '@datahub/metadata-types/constants/common/hadoop-cluster-type';

/**
 * Azkaban cluster information which includes azkaban cluster name and the hadoop cluster
 * @export
 * @interface IAzkabanClusterInfo
 * @link https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/metadata-models/+/master/metadata-models/src/main/pegasus/com/linkedin/dataJob/azkaban/AzkabanClusterInfo.pdsc
 */
export interface IAzkabanClusterInfo {
  // Azkaban cluster name, e.g. ltx1-holdem
  clusterName: string;
  // Hadoop cluster in which the project and flows exist, e.g. HOLDEM, FARO or WAR
  hadoopCluster: HadoopClusterType;
}
