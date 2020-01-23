import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { isDataModelBaseEntityName, DataModelEntity, DataModelName } from '@datahub/data-models/constants/entity';
import { supportedListEntities } from '@datahub/lists/constants/entity/shared';
import Transition from '@ember/routing/-private/transition';

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
    if (
      entity &&
      isDataModelBaseEntityName(entity) &&
      (supportedListEntities as Array<DataModelEntity>).includes(DataModelEntity[entity]) // Allow includes check for all data model entity types
    ) {
      return;
    }

    throw new Error(`Entity ${entity} is not supported`);
  }
}
