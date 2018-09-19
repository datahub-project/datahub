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

  /**
   * Makes an API call and process search entries
   */
  async model(apiParams: any): Promise<{ keywords: string; data: Array<any> }> {
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
    transition.promise.finally(function() {
      controller.set('searchLoading', false);
    });
  }
}
