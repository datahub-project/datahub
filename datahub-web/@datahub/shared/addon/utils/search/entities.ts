import { DataModelEntity } from '@datahub/data-models/constants/entity';

/**
 * Guard function checks if the selected entity isEnabled for search
 * @param {DataModelEntity} entityClass the entity class to check against
 * TODO: META-10750 Refactor out by making the search attribute in IEntityRenderPropsSearch optional,
 * updating call sites with sensible defaults and adding test coverage
 */
export const isSearchable = (entityClass: DataModelEntity): boolean =>
  Boolean(entityClass.renderProps?.search?.isEnabled);
