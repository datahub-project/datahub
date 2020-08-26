import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { inject as service } from '@ember/service';
import { DataModelName } from '@datahub/data-models/constants/entity';
import RouterService from '@ember/routing/router-service';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import { IBrowserRouteParams } from '@datahub/shared/types/browse/browse';
import DataModelsService from '@datahub/data-models/services/data-models';
import { IBrowseEntityModel } from '@datahub/shared/components/browser/containers/entity-categories';

const queryParamsKeys: Array<keyof IBrowserRouteParams> = ['page', 'path', 'size'];

/**
 * Index page for browsing inside an entity.
 */
export default class BrowseEntity extends Route.extend(AuthenticatedRouteMixin) {
  queryParams = refreshModelForQueryParams(queryParamsKeys);

  @service
  router!: RouterService;

  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * Adds a flag guard for entities that are implemented with BaseEntity class and defined in DataModelEntity enumeration,
   * to route paramter values which are described in IBrowseEntity
   * @param params
   */
  model(params: IBrowserRouteParams): IBrowseEntityModel {
    this.checkEntitySupport(params.entity);
    const { dataModels } = this;
    const { page = 1, size = 10, entity, path } = params;
    const dataModelEntity = dataModels.getModel(entity);
    const {
      renderProps: { browse }
    } = dataModelEntity;
    if (browse) {
      const { showHierarchySearch } = browse;
      const segments = String(path || '')
        .split('/')
        .filter(Boolean);

      let dataModelAttributes: Partial<IBrowseEntityModel> = {};

      dataModelAttributes = {
        segments,
        displayName: dataModelEntity.displayName || '',
        showHierarchySearch
      };

      return {
        entity,
        page,
        size,
        ...dataModelAttributes
      };
    }

    throw new Error(`Entity ${entity} not supported`);
  }

  /**
   * Checks if a test string is a known entity and applies the value to the entity attribute on
   * the search service
   * @param {string} entity the entity string to to test against a known list of entities
   * @returns {Entity}
   * @memberof BrowseEntity
   */
  checkEntitySupport(entity: DataModelName): string {
    const entityModelClass = this.dataModels.getModel(entity);
    // checkEntitySupport does not directly impact what the users see on the UI, it only acts as a
    // prevention method (throws error). Because this is only a check that strictly throws an error
    // it would be better to loosen the check a little instead of being so strict that an entity
    // must pass !isGuardedEntity() first.
    // TODO: [META-11590]
    // This is a temporary fix to get some features through, the better method is to allow entities
    // to opt in or out of search and browse through the entity definition, in addition to a check
    // for flag guards.
    if (!this.dataModels.guards.isGuardedEntity(entity) || entityModelClass?.renderProps?.browse) {
      return entity;
    }

    throw new Error(`Entity ${entity} not supported`);
  }
}
