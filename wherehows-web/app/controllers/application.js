import Ember from 'ember';

const {
  get,
  set,
  Controller,
  inject: { service }
} = Ember;

export default Controller.extend({
  /**
   * User session management service
   * @type {service}
   */
  session: service(),
  /**
   * Injected global search service
   * @type {service}
   */
  search: service(),
  /**
   * Service to retrieve type ahead keywords for a dataset
   * @type {service}
   */
  keywords: service('search-keywords'),

  init() {
    this._super(...arguments);
    get(this, 'keywords.keywordsFor')();

    set(this, 'sourceKeywords', get(this, 'keywords.queryResolver'));
  },

  actions: {
    /**
     * Invokes the search service api to transition to the
     *   search results page with the search string
     * @param {String} [keyword] the search string to search for
     */
    didSearch(keyword) {
      get(this, 'search').showSearchResults({ keyword });
    }
  }
});
