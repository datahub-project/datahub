import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import createSearchEntries from 'wherehows-web/utils/datasets/create-search-entries';
import { refreshModelForQueryParams } from 'wherehows-web/utils/helpers/routes';
import { readSearch } from 'wherehows-web/utils/api/search/search';
import { action } from '@ember-decorators/object';
import { service } from '@ember-decorators/service';
import SearchService from 'wherehows-web/services/search';
import { set } from '@ember/object';

export default class SearchRoute extends Route.extend(AuthenticatedRouteMixin) {
  @service
  search: SearchService;

  // Set `refreshModel` for each queryParam to true
  //  so each url state change results in a full transition
  queryParams = refreshModelForQueryParams(['category', 'page', 'facets', 'keyword']);

  /**
   * Makes an API call and process search entries
   */
  async model(apiParams: any): Promise<{ keywords: string; data: Array<any> } | object> {
    const { result } = await readSearch(apiParams);
    const { keywords, data } = result || { keywords: '', data: [] };

    createSearchEntries(data, keywords);
    set(this.search, 'keyword', keywords);
    return result || {};
  }

  /**
   * Add spinner when model is loading
   */
  @action
  loading(transition: import('ember').Ember.Transition): void {
    let controller = this.controllerFor('search');
    set(controller, 'searchLoading', true);
    transition.promise!.finally(function() {
      set(controller, 'searchLoading', false);
    });
  }

  /**
   * In order to keep the service up date with the state. The router pass
   * the keyword from the queryParams to the service.
   * @param transition Ember transition
   */
  @action
  willTransition(transition: import('ember').Ember.Transition & { targetName: string }) {
    if (transition.targetName !== 'search') {
      set(this.search, 'keyword', '');
    }
  }
}
