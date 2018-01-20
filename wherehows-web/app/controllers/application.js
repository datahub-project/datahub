import Controller from '@ember/controller';
import { get } from '@ember/object';
import { inject as service } from '@ember/service';

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
   * Looks up user names and properties from the partyEntities api
   * @type {Ember.Service}
   */
  ldapUsers: service('user-lookup'),

  notifications: service(),

  init() {
    this._super(...arguments);

    // Primes the cache for keywords and userEntitiesSource
    get(this, 'ldapUsers.fetchUserNames')();
  },

  actions: {
    /**
     * Invokes the search service api to transition to the
     *   search results page with the search string
     * @param {String} [keyword] the search string to search for
     * @param {String} [category] restrict search to results found here
     */
    didSearch({ keyword, category }) {
      get(this, 'search').showSearchResults({ keyword, category });
    }
  }
});
