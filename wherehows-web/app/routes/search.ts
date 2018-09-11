import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import createSearchEntries from 'wherehows-web/utils/datasets/create-search-entries';
import { refreshModelQueryParams } from 'wherehows-web/utils/helpers/routes';
import SearchController from 'wherehows-web/controllers/search';
import { readSearch, ISearchApiParams } from 'wherehows-web/utils/api/search';

export default class SearchRoute extends Route.extend(AuthenticatedRouteMixin) {
  // Set `refreshModel` for each queryParam to true
  //  so each url state change results in a full transition
  queryParams = refreshModelQueryParams(['keyword', 'category', 'page', 'source']);

  /**
   * Applies the returned results object as the route model and sets
   *   keyword property on the route controller
   * @param {Object} controller search route controller
   * @param {Object} model search results
   */
  setupController(controller: SearchController, model: any) {
    const { keywords } = model;

    controller.setProperties({
      model,
      keyword: keywords
    });
  }

  /**
   * Makes an API call and process search entries
   */
  async model(apiParams: ISearchApiParams) {
    const { result } = await readSearch(apiParams);
    const { keywords, data } = result;
    createSearchEntries(data, keywords);
    return result;
  }
}
