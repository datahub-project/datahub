import { IBaseEntity } from '@datahub/metadata-types/types/entity';
import { IAzkabanClusterInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/azkaban-cluster-info';
import { IAzkabanProjectInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/azkaban-project-info';
import { IAzkabanFlowInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/azkaban-flow-info';
import { IAzkabanFlowJobsInfo } from '@datahub/metadata-types/types/entity/data-job/azkaban/flow/azkaban-flow-jobs-info';

/**
 * Describes the mid-tier interface for the Azkaban flow entity
 * @export
 * @interface IAzkabanFlowEntity
 * @extends {IBaseEntity}
 */
export interface IAzkabanFlowEntity extends IBaseEntity {
  // Azkaban jobs associated with a Azkaban flow
  jobsInfo: IAzkabanFlowJobsInfo;
  // Azkaban cluster name, e.g. ltx1-holdem
  clusterName: IAzkabanClusterInfo['clusterName'];
  // Azkaban project name
  projectName: IAzkabanProjectInfo['projectName'];
  // Azkaban flow name
  flowName: IAzkabanFlowInfo['flowName'];
  // Metadata information associated with a Azkaban flow
  info: {
    // Metadata associated with a Azkaban project
    project: IAzkabanProjectInfo;
    // Azkaban flow name
    flowName: IAzkabanFlowInfo['flowName'];
  };
}

// Alias for the interface attributes returned by the mid-tier endpoint for Azkaban Flow instance
export type AzkabanFlowApiEntity = Omit<IAzkabanFlowEntity, 'removed'>;
