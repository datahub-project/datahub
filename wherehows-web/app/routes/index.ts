import Route from '@ember/routing/route';
import { get } from '@ember/object';
import { service } from '@ember-decorators/service';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import Configurator from 'wherehows-web/services/configurator';
import CurrentUser from 'wherehows-web/services/current-user';
import Metrics from 'ember-metrics';

export default class IndexRoute extends Route.extend(AuthenticatedRouteMixin) {
  /**
   * @type {CurrentUser}
   */
  @service('current-user')
  sessionUser: CurrentUser;

  /**
   * Metrics tracking service
   * @type {Metrics}
   */
  @service
  metrics: Metrics;

  /**
   * Perform post model operations
   * @param {import('ember').Ember.Transition} transition
   * @return {Promise}
   */
  async beforeModel(transition: import('ember').Ember.Transition) {
    super.beforeModel(transition);

    await this._trackCurrentUser();
    this.replaceWithBrowseDatasetsRoute();
  }

  replaceWithBrowseDatasetsRoute() {
    this.replaceWith('browse.entity', 'datasets');
  }

  /**
   * On entry into route, track the currently logged in user
   * @return {Promise.<void>}
   * @private
   */
  async _trackCurrentUser() {
    const { tracking } = await Configurator.getConfig<undefined>();

    // Check if tracking is enabled prior to invoking
    // Passes an anonymous function to track the currently logged in user using the singleton `current-user` service
    return (
      tracking &&
      tracking.isEnabled &&
      get(this, 'sessionUser').trackCurrentUser(userId => get(this, 'metrics').identify({ userId }))
    );
  }
}
