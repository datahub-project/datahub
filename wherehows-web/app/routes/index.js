import Route from '@ember/routing/route';
import { get } from '@ember/object';
import { inject } from '@ember/service';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { featureEntryPoints } from 'wherehows-web/constants/application';
import Configurator from 'wherehows-web/services/configurator';

export default Route.extend(AuthenticatedRouteMixin, {
  /**
   * @type {Ember.Service}
   */
  sessionUser: inject('current-user'),

  /**
   * Metrics tracking service
   * @type {Ember.Service}
   */
  metrics: inject(),

  /**
   * Perform post model operations
   * @return {Promise}
   */
  async beforeModel() {
    this._super(...arguments);

    await this._trackCurrentUser();
    this.replaceWithBrowseDatasetsRoute();
  },

  replaceWithBrowseDatasetsRoute() {
    this.replaceWith('browse.entity', 'datasets');
  },

  /**
   * On entry into route, track the currently logged in user
   * @return {Promise.<void>}
   * @private
   */
  async _trackCurrentUser() {
    const { tracking = {} } = await Configurator.getConfig();

    // Check if tracking is enabled prior to invoking
    // Passes an anonymous function to track the currently logged in user using the singleton `current-user` service
    return (
      tracking.isEnabled &&
      get(this, 'sessionUser').trackCurrentUser(userId => get(this, 'metrics').identify({ userId }))
    );
  }
});
