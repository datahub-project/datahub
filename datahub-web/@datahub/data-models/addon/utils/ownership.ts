import { DataModelEntity } from '@datahub/data-models/constants/entity/index';

/**
 * Given a data model entity, returns whether that entity is valid for ownership
 * @param {DataModelEntity} entity - data model entity class we want to test
 */
export const isOwnableEntity = (entity: DataModelEntity): boolean => Boolean(entity.renderProps?.userEntityOwnership);
