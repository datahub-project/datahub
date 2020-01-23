import { IAppConfig, IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

/**
 * Filters out DataModelEntity types that have configurator guard values set to false
 */
export const unGuardedEntities = (getConfig: IConfigurator['getConfig']): Array<DataModelEntity> => {
  // Map of Entity display names to configurator flags
  const guards: Record<PersonEntity['displayName'], keyof IAppConfig> = {
    [PersonEntity.displayName]: 'showPeople'
  };

  // List of DataModeEntities that are not flag guarded
  const unGuardedEntities: Array<DataModelEntity> = [];

  return Object.values(DataModelEntity).reduce(
    (unGuardedEntities, entity: typeof DataModelEntity[keyof typeof guards]): Array<DataModelEntity> => {
      const guard = guards[entity.displayName];
      const isGuarded = Boolean(guard && !getConfig(guard));

      return isGuarded ? unGuardedEntities : [...unGuardedEntities, entity];
    },
    unGuardedEntities
  );
};
