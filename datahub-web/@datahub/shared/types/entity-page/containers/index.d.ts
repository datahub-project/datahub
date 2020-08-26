import { ETaskPromise, ETask } from '@datahub/utils/types/concurrency';
import { ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';

/**
 * Defines expected container properties and methods for an Entity Container Component
 * This allows a uniform base contract when dealing with entity top level containers
 * @interface IEntityContainer
 * @template T DataModelEntity which the container is responsible for
 */
export interface IEntityContainer<T> {
  // urn for the entity
  urn?: string;
  // Reference to the underlying entity
  entity?: T;
  // The currently selected or current tab location
  currentTab?: string;
  // Tabs that are available for the entity
  tabs: Array<ITabProperties>;
  // Ember Concurrency task to materialize the related underlying IEntity, usually by creating an instance,
  // invoking the readEntity method and setting this value in the container
  reifyEntityTask: ETaskPromise<T> | ETask<T>;
}
