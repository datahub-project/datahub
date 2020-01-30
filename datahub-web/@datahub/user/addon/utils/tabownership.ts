import { DataModelEntity } from '@datahub/data-models/constants/entity';
import DataModelsService from '@datahub/data-models/services/data-models';

/**
 * Given an entity, it will generate the ownership tab id for that entity
 * @param entity it will be used to get the displayName
 */
export const generateTabId = (entity: DataModelEntity): string => `userownership-${entity.displayName}`;

/**
 * Reverse fn of `generateTabId`. Given an Id (and dataModelsService), will return the entity for that tab
 * @param tabId tabId for the entity
 * @param dataModels Service to transform the displayName into the entity
 */
export const entityFromTabId = (tabId: string, dataModels: DataModelsService): DataModelEntity =>
  dataModels.getModel(tabId.replace('userownership-', '') as DataModelEntity['displayName']);
