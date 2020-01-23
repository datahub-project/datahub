import { IDatasetView, IDataset } from 'wherehows-web/typings/api/datasets/dataset';
import { ISharedOwner } from 'wherehows-web/typings/api/ownership/owner';
import { IMirageServer, IMirageDBs, IMirageDB } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';

/**
 * Specific list of datasets for wherehows
 */
export interface IMirageWherehowsDBs extends IMirageDBs {
  datasets: IMirageDB<IDataset>;
  datasetViews: IMirageDB<IDatasetView>;
  datasetOwnerships: IMirageDB<ISharedOwner & { datasetId: string }>; // so we can join with dataset
  platforms: IMirageDB<IDataPlatform>;
}

/**
 * Alias for wherehows
 */
export type IMirageWherehows = IMirageServer<IMirageWherehowsDBs>;
