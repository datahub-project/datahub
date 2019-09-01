import { HadoopClusterType } from '@datahub/metadata-types/constants/common/hadoop-cluster-type';

/**
 * Schedule properties when the flow is run. This is chosen based on the availability of input data sources
 * @namespace Dataset
 * @interface IScheduleProperties
 */
export interface IScheduleProperties {
  // Cluster qualifier for this schedule
  value: string;
  // Cluster qualifier for this schedule
  cluster: HadoopClusterType;
}
