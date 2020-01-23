import { ITabProperties, Tab } from '@datahub/data-models/constants/entity/shared/tabs';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

/**
 * Defines expected container properties and methods for an Entity Container Component
 * This allows a uniform base contract when dealing with entity top level containers
 * @interface IEntityContainer
 * @template T
 */
export interface IEntityContainer<T> {
  // urn for the entity
  urn?: string;
  // Reference to the underlying entity
  entity?: T;
  // The currently selected or current tab location
  currentTab: Tab;
  // Tabs that are available for the entity
  tabs: Array<ITabProperties>;
  // concurrency task to materialize the related underlying IEntity
  reifyEntityTask: ETaskPromise<T>;
}
