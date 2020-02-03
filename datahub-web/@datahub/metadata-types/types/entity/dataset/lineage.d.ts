import { IDatasetEntity } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';

/**
 * Relationship upstream and downstream api will return Array<IDatasetLineage> displaying
 * the type of dataset, type of lineage, and actor urn that modified that relationship
 */
export interface IDatasetLineage {
  dataset: IDatasetEntity;
  type: string;
  actor: string;
}

export type DatasetLineageList = Array<IDatasetLineage>;
