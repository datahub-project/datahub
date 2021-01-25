import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { ListEntity } from '@datahub/data-models/entity/list/list-entity';
import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { IBaseEntity } from '@datahub/metadata-types/types/entity/index';
import { DataConstructChangeManagementEntity } from '@datahub/data-models/entity/data-construct-change-management/data-construct-change-management-entity';
import { GroupEntity } from '@datahub/data-models/entity/group/group-entity';

/**
 * Defines the interface for the DataModelEntity enum below.
 * This allows each entry in the enum to be indexable
 */
export interface IDataModelEntity {
  [DatasetEntity.displayName]: typeof DatasetEntity;
  [PersonEntity.displayName]: typeof PersonEntity;
  [ListEntity.displayName]: typeof ListEntity;
  [GroupEntity.displayName]: typeof GroupEntity;
  [DataConstructChangeManagementEntity.displayName]: typeof DataConstructChangeManagementEntity;
}

/**
 * Enumeration of data model entity displayName values to the related Data Model entity
 * Serves as the primary resource map of all DataModelEntity available classes
 */
export const DataModelEntity: IDataModelEntity = {
  [DatasetEntity.displayName]: DatasetEntity,
  [PersonEntity.displayName]: PersonEntity,
  [GroupEntity.displayName]: GroupEntity,
  [ListEntity.displayName]: ListEntity,
  [DataConstructChangeManagementEntity.displayName]: DataConstructChangeManagementEntity
};

/**
 * Will Generate a map of entities using the api name. Since some entities will throw exceptions while
 * accesing their render props, it will ignore those entities
 * @param entities
 */
export const generateEntityApiNameMap = <k extends IDataModelEntity>(entities: k): Record<string, k[keyof k]> =>
  Object.values(entities).reduce((m, entity: IDataModelEntity[keyof IDataModelEntity]) => {
    // some entities may no implement render props
    try {
      return { ...m, [`${entity.renderProps.apiEntityName}`]: entity };
    } catch (e) {
      return m;
    }
  }, {});

/**
 * Reverse lookup by apiName
 */
export const DataModelEntityApiNameMap: Record<
  string,
  IDataModelEntity[keyof IDataModelEntity]
> = generateEntityApiNameMap(DataModelEntity);
/**
 * Aliases the keys on the DataModelEntity enum for reference convenience
 * This maps to the names of the available entities for example 'datasets',  'users', etc
 */
export type DataModelName = keyof typeof DataModelEntity;

/**
 * Aliases the DataModelEntity classes found in the DataModelEntity enum, this is a union type of all entity classifiers
 * For example { typeof DatasetEntity | typeof UserEntity | ... }
 */
export type DataModelEntity = typeof DataModelEntity[DataModelName];

/**
 * A specific instance of data model entity
 * For example { DatasetEntity | UserEntity | ... }
 * As we move to a dynamic world of entities. Having a more open entity like BaseEntity, which all entities should
 * implement aliviate some type constrains.
 */
export type DataModelEntityInstance = BaseEntity<IBaseEntity | {}>;
