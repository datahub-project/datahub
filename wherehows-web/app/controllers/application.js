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
   * @type {Ember.Service}
   */
  session: service(),
  /**
   * Injected global search service
   * @type {Ember.Service}
   */
  search: service(),
  /**
   * Service to retrieve type ahead keywords for a dataset
   * @type {Ember.Service}
   */
  keywords: service('search-keywords'),

  /**
   * Looks up user names and properties from the partyEntities api
   * @type {Ember.Service}
   */
  ldapUsers: service('user-lookup'),

  init() {
    this._super(...arguments);

    // Primes the cache for keywords and userEntitiesSource
    get(this, 'keywords.keywordsFor')();
    get(this, 'ldapUsers.fetchUserNames')();

    // Sets a reference to the query resolver function that's
    //   accessible by the global-search component rendered in the
    //   application template
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
