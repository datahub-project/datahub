import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import createSearchEntries from 'wherehows-web/utils/datasets/create-search-entries';
import { refreshModelForQueryParams } from 'wherehows-web/utils/helpers/routes';
import { readSearch } from 'wherehows-web/utils/api/search';
import { action } from '@ember-decorators/object';

export default class SearchRoute extends Route.extend(AuthenticatedRouteMixin) {
  // Set `refreshModel` for each queryParam to true
  //  so each url state change results in a full transition
  queryParams = refreshModelForQueryParams(['category', 'page', 'facets', 'keyword']);

  // /**
  //  * Transition to the search route including search keyword as query parameter
  //  * @param {Object} args = {} a map of query parameters to values, including keyword
  //  * @prop {String|*} args.keyword the string to search for
  //  * @returns {void|Transition}
  //  */
  // showSearchResults(args: { keyword: string; category: string }) {
  //   let { keyword, category } = args;

  //   // Transition to search route only if value is not null or void
  //   if (!isBlank(keyword)) {
  //     // Lookup application Route on ApplicationInstance
  //     const applicationRoute = getOwner(this).lookup('route:application');
  //     keyword = encode(keyword);

  //     return applicationRoute.transitionTo('search', {
  //       queryParams: { keyword, category, page: 1, facets: '' }
  //     });
  //   }
  // }

  /**
   * Makes an API call and process search entries
   */
  async model(apiParams: any): Promise<{ keywords: string; data: Array<any> } | object> {
    const { result } = await readSearch(apiParams);
    const { keywords, data } = result || { keywords: '', data: [] };
    createSearchEntries(data, keywords);
    return result || {};
  }

  /**
   * Add spinner when model is loading
   */
  @action
  loading(transition: import('ember').Ember.Transition): void {
    let controller = this.controllerFor('search');
    controller.set('searchLoading', true);
    transition.promise!.finally(function() {
      controller.set('searchLoading', false);
    });
  }
}
