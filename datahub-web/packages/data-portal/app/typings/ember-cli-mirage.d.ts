import { ISharedOwner } from '@datahub/metadata-types/types/common/shared-owner';
import { IMirageServer, IMirageDBs, IMirageDB } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';

/**
 * Specific list of datasets for wherehows
 */
export interface IMirageWherehowsDBs extends IMirageDBs {
  datasets: IMirageDB<Com.Linkedin.Dataset.Dataset>;
  datasetUmps: IMirageDB<Com.Linkedin.Dataset.Ump.UMPDatasetProperties & { datasetId: string }>; // so we can join with dataset
  datasetOwnerships: IMirageDB<ISharedOwner & { datasetId: string }>; // so we can join with dataset
  metrics: IMirageDB<Com.Linkedin.Metric.Metric>;
  platforms: IMirageDB<IDataPlatform>;
  features: IMirageDB<Com.Linkedin.Metadata.Entity.FeatureEntity>;
}

/**
 * Alias for wherehows
 */
export type IMirageWherehows = IMirageServer<IMirageWherehowsDBs>;
