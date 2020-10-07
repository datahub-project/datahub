import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { DataModelName } from '@datahub/data-models/constants/entity';
import Transition from '@ember/routing/-private/transition';
import { inject as service } from '@ember/service';
import DataModelsService from '@datahub/data-models/services/data-models';

/**
 * Describes the ListEntity route transition object parameters
 */
interface IListEntityTransitionParams {
  entity_name?: DataModelName;
}

/**
 * Defines the route for Entity Lists, enabling navigation to a Lists page for a known / supported entities
 * @export
 * @class ListsEntity
 * @extends {Route.extend(AuthenticatedRouteMixin)}
 */
export default class ListsEntity extends Route.extend(AuthenticatedRouteMixin) {
  /**
   * ListsEntity will use Data Models service to check if the entity is guarded or not
   */
  @service('data-models')
  dataModels: DataModelsService;

  /**
   * Before loading the model we will check if list supports this entity type
   * @param transition
   */
  beforeModel(transition: Transition): void {
    // eslint-disable-next-line @typescript-eslint/camelcase
    const { entity_name: entity }: IListEntityTransitionParams =
      (transition.to && (transition.to.params as IListEntityTransitionParams)) || {};
    this.checkEntitySupport(entity);
  }

  /**
   * Returns the entity_name parameter as a entityType
   */
  model({ entity_name: entityType }: IListEntityTransitionParams): { entityType?: string } {
    return { entityType };
  }

  /**
   * Checks that the entity supplied in the route paramter is an entity that has support for Entity Lists
   * @memberof ListsEntity
   */
  checkEntitySupport(entity?: DataModelName): void {
    if (entity && !this.dataModels.guards.isGuardedEntity(entity)) {
      return;
    }

    throw new Error(`Entity ${entity} is not supported`);
  }
}
