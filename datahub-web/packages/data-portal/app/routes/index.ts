import Route from '@ember/routing/route';
import { get } from '@ember/object';
import { inject as service } from '@ember/service';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import CurrentUser from '@datahub/shared/services/current-user';
import Metrics from 'ember-metrics';
import { getConfig } from '@datahub/shared/services/configurator';
import Transition from '@ember/routing/-private/transition';

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
   * @param {EmberTransition} transition
   * @return {Promise}
   */
  async beforeModel(transition: Transition): Promise<void> {
    super.beforeModel(transition);

    // Track returning user i.e. entry via non-login route
    await this._trackCurrentUser();
    this.replaceWithBrowseSubRoute();
  }

  /**
   * Transition to sub route browse and replace route on navigation
   * The index route is currently an unused builtin route
   */
  replaceWithBrowseSubRoute(): void {
    this.replaceWith('browse');
  }

  /**
   * On entry into route, track the currently logged in user
   * @return {Promise.<void>}
   * @private
   */
  async _trackCurrentUser(): Promise<false | void> {
    const { tracking } = await getConfig<undefined>();

    // Check if tracking is enabled prior to invoking
    // Passes an anonymous function to track the currently logged in user using the singleton `current-user` service
    return (
      tracking &&
      tracking.isEnabled &&
      get(this, 'sessionUser').trackCurrentUser((userId): void => get(this, 'metrics').identify({ userId }))
    );
  }
}
