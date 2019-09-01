import { IAppConfig } from 'wherehows-web/typings/api/configurator/configurator';
import Configurator from 'wherehows-web/services/configurator';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

/**
 * Filters out DataModelEntity types that have configurator guard values set to false
 */
export const unGuardedEntities = (configurator: typeof Configurator): Array<DataModelEntity> => {
  // Map of Entity display names to configurator flags
  const guards: Partial<Record<DatasetEntity['displayName'], keyof IAppConfig>> = {};

  // List of DataModeEntities that are not flag guarded
  const unGuardedEntities: Array<DataModelEntity> = [];

  return Object.values(DataModelEntity).reduce(
    (unGuardedEntities, entity: typeof DataModelEntity[keyof typeof guards]): Array<DataModelEntity> => {
      const guard = guards[entity.displayName];
      const isGuarded = Boolean(guard && !configurator.getConfig(guard));

      return isGuarded ? unGuardedEntities : [...unGuardedEntities, entity];
    },
    unGuardedEntities
  );
};
