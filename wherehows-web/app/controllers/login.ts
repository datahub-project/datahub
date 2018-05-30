import Controller from '@ember/controller';
import { computed, get, setProperties, getProperties } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import { inject as service } from '@ember/service';
import Session from 'ember-simple-auth/services/session';
import { twoFABannerMessage, twoFABannerType } from 'wherehows-web/constants/notifications';
import BannerService from 'wherehows-web/services/banners';

export default class Login extends Controller {
  /**
   * References the application session service
   * @type {ComputedProperty<Session>}
   * @memberof Login
   */
  session: ComputedProperty<Session> = service();

  /**
   * Banner alert service
   * @type {Ember.Service}
   */
  banners: ComputedProperty<BannerService> = service();

  /**
   * Aliases the name property on the component
   * @type {ComputedProperty<string>}
   * @memberof Login
   */
  username = computed.alias('name');

  /**
   * Aliases the password computed property on the component
   * @type {ComputedProperty<string>}
   * @memberof Login
   */
  password = computed.alias('pass');

  /**
   * On instantiation, error message reference is an empty string value
   * @type {string}
   * @memberof Login
   */
  errorMessage = '';

  actions = {
    /**
     * Using the session service, authenticate using the custom ldap authenticator
     * @return {void}
     */
    authenticateUser(this: Login): void {
      const { username, password, banners } = getProperties(this, ['username', 'password', 'banners']);

      get(this, 'session')
        .authenticate('authenticator:custom-ldap', username, password)
        .then(results => {
          // Once user has chosen to authenticate, then we remove the login banner (if it exists) since it will
          // no longer be relevant
          banners.removeBanner(twoFABannerMessage, twoFABannerType);
          return results;
        })
        .catch(({ responseText = 'Bad Credentials' }) => setProperties(this, { errorMessage: responseText }));
    }
  };
}
