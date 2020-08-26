import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import DataModelsService from '@datahub/data-models/services/data-models';
import { inject as service } from '@ember/service';
import { DataModelName, DataModelEntity } from '@datahub/data-models/constants/entity';

export interface IEntityTypeRouteModel {
  entityClass: DataModelEntity;
}

/**
 * Root route for the entity page.
 *
 * This route will read the entity type from the URL. Entity type will be used to get the EntityModel.
 * It will also check if the entity is setup to show an entity page using the EntityModel.
 *
 * In case of not able to find a model, or entityPage not defined, will render 'Page not found'
 *
 * The model will contain a reference to the entityClass (returned by data-models), or void, in case
 * of not finding the entity.
 */
export default class EntityType extends Route.extend(AuthenticatedRouteMixin) {
  /**
   * Data Models service to get the entity model
   */
  @service
  dataModels!: DataModelsService;

  /**
   * entity_type is a string and it might not be a DataModelName
   */
  model({ entity_type: entityType }: { entity_type: DataModelName }): IEntityTypeRouteModel | void {
    const { dataModels } = this;
    const isGuardedEntity = dataModels.guards.isGuardedEntity(entityType);
    const entityClass = dataModels.getModel(entityType);
    const hasEntityPageComponent = Boolean(
      entityClass &&
        entityClass.renderProps &&
        entityClass.renderProps.entityPage &&
        entityClass.renderProps.entityPage.pageComponent
    );
    if (!isGuardedEntity && entityClass && hasEntityPageComponent) {
      return { entityClass };
    }
  }
}
