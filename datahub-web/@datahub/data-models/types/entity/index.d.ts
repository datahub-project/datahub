import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

/**
 * Algebraic data type of entity product types. New Entity types should be added here
 * Included union types are discriminated on the "kind" field
 */
export type Entity = DatasetEntity;
