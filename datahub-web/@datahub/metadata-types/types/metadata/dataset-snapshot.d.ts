import { IDatasetAspect } from '@datahub/metadata-types/types/entity/dataset/aspect';

/**
 * A metadata snapshot for a specific dataset entity
 * @export
 * @namespace metadata.snapshot
 * @interface IDatasetSnapshot
 */
export interface IDatasetSnapshot {
  // URN for the entity the metadata snapshot is associated with
  urn: string;
  // The list of metadata aspects associated with the dataset.
  // Depending on the use case, this can either be all, or a selection of supported aspects
  aspects: Array<IDatasetAspect>;
}
