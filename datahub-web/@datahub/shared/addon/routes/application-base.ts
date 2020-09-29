import ApplicationRouteMixin from 'ember-simple-auth/mixins/application-route-mixin';
import Route from '@ember/routing/route';
import CurrentUser from '@datahub/shared/services/current-user';
import { inject as service } from '@ember/service';
import { IAppConfig } from '@datahub/shared/types/configurator/configurator';
import Configurator from '@datahub/shared/services/configurator';

export default class ApplicationBaseRoute extends Route.extend(ApplicationRouteMixin) {
  /**
   * Injected service to manage the operations related to currently logged in user
   * On app load, avatar creation and loading user attributes are handled via this service
   */
  @service('current-user')
  sessionUser!: CurrentUser;

  @service
  configurator!: Configurator;

  /**
   * Attempt to load the current user and application configuration options
   * @returns {Promise}
   */
  beforeModel(...args: Array<unknown>): Promise<[void, IAppConfig]> {
    super.beforeModel.apply(this, args);
    return Promise.all([this._loadCurrentUser(), this._loadConfig()]);
  }

  /**
   * Loads the application configuration object
   * @return {Promise.<any>|void}
   * @private
   */
  _loadConfig(): Promise<IAppConfig> {
    return this.configurator.load();
  }
  /**
   * Internal method to invoke the currentUser service's load method
   *   If an exception occurs during the load for the current user,
   *   invalidate the session.
   * @private
   */
  async _loadCurrentUser(): Promise<void> {
    const { sessionUser } = this;

    try {
      await sessionUser.load();
    } catch {
      sessionUser.invalidateSession();
    }
  }

  /**
   * Augments sessionAuthenticated.
   * @override ApplicationRouteMixin.sessionAuthenticated
   */
  async sessionAuthenticated(...args: Array<unknown>): Promise<void> {
    // @ts-ignore waiting for this be solved: https://github.com/simplabs/ember-simple-auth/issues/1619
    super.sessionAuthenticated.apply(this, args);
    await this._loadCurrentUser();
  }
}
