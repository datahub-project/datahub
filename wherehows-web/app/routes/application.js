import Ember from 'ember';
import ApplicationRouteMixin
  from 'ember-simple-auth/mixins/application-route-mixin';

const {
  Route,
  run,
  get,
  inject: { service }
} = Ember;

export default Route.extend(ApplicationRouteMixin, {
  // Injected Ember#Service for the current user
  sessionUser: service('current-user'),

  /**
   * Attempt to load the current user
   * @returns {Promise}
   */
  beforeModel() {
    return this._loadCurrentUser();
  },

  /**
   * Augments sessionAuthenticated.
   * @override ApplicationRouteMixin.sessionAuthenticated
   */
  sessionAuthenticated() {
    this._super(...arguments);
    this._loadCurrentUser();
  },

  /**
   * Internal method to invoke the currentUser service's load method
   *   If an exception occurs during the load for the current user,
   *   invalidate the session.
   * @returns {Promise<T, V>|RSVP.Promise<any>|Ember.RSVP.Promise<any, any>|Promise.<T>}
   * @private
   */
  _loadCurrentUser() {
    return get(this, 'sessionUser')
      .load()
      .catch(() => get(this, 'session').invalidate());
  },

  init() {
    this._super(...arguments);
    run.scheduleOnce('afterRender', this, 'processLegacyDomOperations');
  },

  processLegacyDomOperations() {
    // TODO: DSS-6122 Refactor Remove tree legacy operations & references
    // window.legacySearch();
    // window.legacyTree();
    window.legacyMain();
  }
});
