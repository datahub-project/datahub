import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import { action } from '@ember/object';
import { inject as service } from '@ember/service';
import SearchService from 'wherehows-web/services/search';
import { set } from '@ember/object';
import { ISearchApiParams } from 'wherehows-web/typings/api/search/search';
import SearchController from 'wherehows-web/controllers/search';
import Transition from '@ember/routing/-private/transition';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/entity/rendering/search-entity-render-prop';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

export default class SearchRoute extends Route.extend(AuthenticatedRouteMixin) {
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
  model(queryParam: ISearchApiParams): { fields: Array<ISearchEntityRenderProps> } {
    set(this.search, 'entity', queryParam.entity || DatasetEntity.displayName);
    set(this.search, 'keyword', queryParam.keyword);
    return { fields: DataModelEntity[queryParam.entity].renderProps.search.attributes };
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
