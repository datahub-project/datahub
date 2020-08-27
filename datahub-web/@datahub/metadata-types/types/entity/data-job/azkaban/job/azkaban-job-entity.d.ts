import { IBaseEntity } from '@datahub/metadata-types/types/entity';
import { IDataJobOutput } from '@datahub/metadata-types/types/entity/data-job/data-job-output';
import { IDataJobInput } from '@datahub/metadata-types/types/entity/data-job/data-job-input';
import { IAzkabanClusterInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/azkaban-cluster-info';
import { IAzkabanProjectInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/azkaban-project-info';
import { IAzkabanJobInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/job/azkaban-job-info';
import { IAzkabanFlowInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/azkaban-flow-info';

/**
 * Describes the mid-tier interface for the Azkaban Job entity
 * @export
 * @interface IAzkabanJobEntity
 * @extends {IBaseEntity}
 */
export interface IAzkabanJobEntity extends IBaseEntity {
  // Identification URN for a specific job instance
  urn: string;
  // Azkaban job name
  jobName: IAzkabanJobInfo['jobName'];
  // Outputs produced by the data job
  output: IDataJobOutput;
  // Inputs consumed by the data job
  input: IDataJobInput;
  // Azkaban cluster name, e.g. ltx1-holdem
  clusterName: IAzkabanClusterInfo['clusterName'];
  // Azkaban project name
  projectName: IAzkabanProjectInfo['projectName'];
  // Azkaban flow name
  flowName: IAzkabanFlowInfo['flowName'];
  // Metadata information associated with a Azkaban job
  info: {
    // Azkaban job name
    jobName: IAzkabanJobInfo['jobName'];
    // Metadata associated with a Azkaban project
    project: IAzkabanProjectInfo;
    // Azkaban flow name
    flowName: IAzkabanFlowInfo['flowName'];
  };
}

// Alias for the interface attributes returned by the mid-tier endpoint for Azkaban Job instance
export type AzkabanJobApiEntity = Omit<IAzkabanJobEntity, 'removed'>;
