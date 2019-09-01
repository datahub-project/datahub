import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { inject as service } from '@ember/service';
import Search from 'wherehows-web/services/search';
import { set } from '@ember/object';
import { isDataModelBaseEntityName, DataModelEntity, DataModelName } from '@datahub/data-models/constants/entity';
import RouterService from '@ember/routing/router-service';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import { IBrowserRouteParams } from 'wherehows-web/typings/app/browse';

const queryParamsKeys: Array<keyof IBrowserRouteParams> = ['page', 'path', 'size'];

/**
 * Describes the properties that are returned from the BrowseEntityCategory route's model hook
 * if the current entity is implemented using the BaseEntity data model class, then properties
 * are augmented with attributes to enable proper rendering
 * @interface IBrowseEntityModel
 */
interface IBrowseEntityModel {
  // Entity display name used in breadcrumbs component
  displayName?: string;
  // category and prefix string values
  segments?: Array<string>;
  entity: DataModelName;
  page: number;
  size: number;
  // References the DataModelEntity for the current entity being browsed
  showHierarchySearch?: boolean;
}

/**
 * Index page for browsing inside an entity.
 */
export default class BrowseEntity extends Route.extend(AuthenticatedRouteMixin) {
  queryParams = refreshModelForQueryParams(queryParamsKeys);
  /**
   * Inject search service to set current entity
   */
  @service
  search: Search;

  @service
  router: RouterService;

  /**
   * Adds a flag guard for entities that are implemented with BaseEntity class and defined in DataModelEntity enumeration,
   * to route paramter values which are described in IBrowseEntity
   * @param params
   */
  model(params: IBrowserRouteParams): IBrowseEntityModel {
    this.checkEntitySupport(params.entity);
    const { page = 1, size = 10, entity, path } = params;
    const {
      renderProps: { browse }
    } = DataModelEntity[entity];
    const { showHierarchySearch } = browse;
    const segments = String(path || '')
      .split('/')
      .filter(Boolean);

    let dataModelAttributes: Partial<IBrowseEntityModel> = {};

    dataModelAttributes = {
      segments,
      displayName: DataModelEntity[entity].displayName || '',
      showHierarchySearch
    };

    return {
      entity,
      page,
      size,
      ...dataModelAttributes
    };
  }

  /**
   * Checks if a test string is a known entity and applies the value to the entity attribute on
   * the search service
   * @param {string} entity the entity string to to test against a known list of entities
   * @returns {Entity}
   * @memberof BrowseEntity
   */
  checkEntitySupport(entity: DataModelName): string {
    if (isDataModelBaseEntityName(entity)) {
      set(this.search, 'entity', entity);
      return entity;
    }

    throw new Error(`Entity ${entity} not supported`);
  }
}
