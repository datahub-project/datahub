import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

/**
 * Defines the interface for the DataModelEntity enum below.
 * This allows each entry in the enum to be indexable
 */
export interface IDataModelEntity {
  [DatasetEntity.displayName]: typeof DatasetEntity;
  [PersonEntity.displayName]: typeof PersonEntity;
}

/**
 * Enumeration of data model entity displayName values to the related Data Model entity
 * Serves as the primary resource map of all DataModelEntity available classes
 */
export const DataModelEntity: IDataModelEntity = {
  [DatasetEntity.displayName]: DatasetEntity,
  [PersonEntity.displayName]: PersonEntity
};

/**
 * Aliases the keys on the DataModelEntity enum for reference convenience
 * This maps to the names of the available entities for example 'datasets',  'users', etc
 */
export type DataModelName = keyof typeof DataModelEntity;

/**
 * Aliases the DataModelEntity classes found in the DataModelEntity enum, this is a union type of all entity classifiers
 * For example { DatasetEntity | UserEntity | ... }
 */
export type DataModelEntity = typeof DataModelEntity[DataModelName];

/**
 * Guards on a string entityName if it maps to an entity data model (class inheriting from BaseEntity)
 * @param {string} entityName the displayName to match against for DataModelEntity types
 */
export const isDataModelBaseEntityName = (entityName: DataModelName): boolean =>
  Object.values(DataModelEntity)
    .mapBy('displayName')
    .includes(entityName);
