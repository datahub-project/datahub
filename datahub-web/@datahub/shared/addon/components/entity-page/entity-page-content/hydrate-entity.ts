import Component from '@glimmer/component';
import { IHydrateEntity } from '@datahub/data-models/types/entity/rendering/page-components';
import { DataModelEntityInstance, DataModelName } from '@datahub/data-models/constants/entity';

import { inject as service } from '@ember/service';
import DataModelsService from '@datahub/data-models/services/data-models';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { task } from 'ember-concurrency';
import { set } from '@ember/object';

interface IEntityPageContentHydrateEntityArgs {
  // Dehydrated entity
  entity: DataModelEntityInstance;
  // Child component that will receive
  options: IHydrateEntity['options'];
}

/**
 * This component will create an entity given an entity... wait what? The problem is that
 * sometimes entity intances are not completely instatiated (with all props). This class will act as
 * a simple container calling DataModels and instanciating a complete instance.
 */
export default class EntityPageContentHydrateEntity extends Component<IEntityPageContentHydrateEntityArgs> {
  /**
   * Data Models service to hydrate entities
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * Output of this component
   */
  hydratedEntity!: DataModelEntityInstance;

  /**
   * Task that will hydrate the entity by calling create instance
   */
  @(task(function*(this: EntityPageContentHydrateEntity): IterableIterator<Promise<DataModelEntityInstance>> {
    const { entity } = this.args;
    const { dataModels } = this;

    if (entity && entity.urn) {
      const hydratedEntity = yield dataModels.createInstance(
        entity.staticInstance.displayName as DataModelName,
        entity.urn
      );
      set(this, 'hydratedEntity', (hydratedEntity as unknown) as DataModelEntityInstance);
    }
  }).restartable())
  hydrateEntityTask!: ETaskPromise<void>;

  /**
   * Will invoke hydrateEntityTask upon creation
   * @param owner passthrough to parent
   * @param args passthrough to parent
   */
  constructor(owner: unknown, args: IEntityPageContentHydrateEntityArgs) {
    super(owner, args);

    this.hydrateEntityTask.perform();
  }
}
