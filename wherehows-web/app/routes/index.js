import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { featureEntryPoints } from 'wherehows-web/constants/application';

const { get, Route, inject: { service } } = Ember;
const { browse, scriptFinder, schemaHistory, idpc } = featureEntryPoints;

export default Route.extend(AuthenticatedRouteMixin, {
  /**
   * @type {Ember.Service}
   */
  sessionUser: service('current-user'),

  /**
   * Runtime application configuration options
   * @type {Ember.Service}
   */
  configurator: service(),

  /**
   * Metrics tracking service
   * @type {Ember.Service}
   */
  metrics: service(),

  model() {
    // Static list of content for the index route featureCard links
    return [browse, scriptFinder, schemaHistory, idpc];
  },
  /**
   * Perform post model operations
   * @return {Promise}
   */
  afterModel() {
    this._super(...arguments);

    return this._trackCurrentUser();
  },

  /**
   * On entry into route, track the currently logged in user
   * @return {Promise.<void>}
   * @private
   */
  async _trackCurrentUser() {
    const { tracking = {} } = await get(this, 'configurator.getConfig')();

    // Check if tracking is enabled prior to invoking
    // Passes an anonymous function to track the currently logged in user using the singleton `current-user` service
    return (
      tracking.isEnabled &&
      get(this, 'sessionUser').trackCurrentUser(userId => get(this, 'metrics').identify({ userId }))
    );
  }
});
