import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import { action, setProperties, set } from '@ember/object';
import { inject as service } from '@ember/service';
import SearchService from '@datahub/shared/services/search';
import SearchController from 'datahub-web/controllers/search';
import Transition from '@ember/routing/-private/transition';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import DataModelsService from '@datahub/data-models/services/data-models';
import { IEntityRenderCommonPropsSearch } from '@datahub/data-models/types/search/search-entity-render-prop';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';
import Configurator from '@datahub/shared/services/configurator';
import { DataModelName } from '@datahub/data-models/constants/entity/index';

export default class SearchRoute extends Route.extend(AuthenticatedRouteMixin) {
  /**
   * Service to get the right class for the entity type
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * Injects the service taht lets us access our application configurations
   */
  @service
  configurator!: Configurator;

  /**
   * Stores a reference to the application search service
   * @type {SearchService}
   * @memberof SearchRoute
   */
  @service
  search: SearchService;

  /**
   * For each listed query parameter, invoke a full transition if the query parameter changes in the url
   * @type {Record<string, {
   *     refreshModel: true;
   * }>}
   * @memberof SearchRoute
   */
  queryParams = refreshModelForQueryParams(['entity', 'page', 'facets', 'keyword']);

  /**
   * Makes an API call and process search entries
   * @param {ISearchApiParams} queryParam
   * @return {void}
   * @memberof SearchRoute
   */
  model(queryParam: {
    entity: DataModelName;
    page: string;
    facets: string;
    keyword: string;
  }): {
    fields: Array<ISearchEntityRenderProps>;
    showFacets: boolean;
    searchConfig: IEntityRenderCommonPropsSearch;
  } {
    const { dataModels } = this;
    const dataModelEntity = dataModels.getModel(queryParam.entity);
    const searchProps = dataModelEntity.renderProps.search;
    setProperties(this.search, {
      entity: queryParam.entity || DatasetEntity.displayName,
      keyword: queryParam.keyword
    });

    return {
      searchConfig: searchProps,
      fields: searchProps.attributes,
      showFacets: typeof searchProps.showFacets === 'boolean' ? searchProps.showFacets : true
    };
  }

  /**
   * Will cleanup controller, so there are no variables leaking.
   * @param controller
   * @param isExiting
   * @param transition
   */
  resetController(controller: SearchController, isExiting: boolean, transition: Transition): void {
    super.resetController(controller, isExiting, transition);

    if (isExiting) {
      controller.resetData();
    }
  }

  /**
   * In order to keep the service up date with the state. The router pass
   * the keyword from the queryParams to the service.
   * @param transition Ember transition
   */
  @action
  willTransition(transition: Transition): void {
    if (transition.to.name !== 'search') {
      set(this.search, 'keyword', '');
    }
  }
}
