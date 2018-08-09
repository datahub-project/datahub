import Controller from '@ember/controller';
import { get, setProperties, getProperties } from '@ember/object';
import Session from 'ember-simple-auth/services/session';
import { twoFABannerMessage, twoFABannerType } from 'wherehows-web/constants/notifications';
import BannerService from 'wherehows-web/services/banners';
import { alias } from '@ember-decorators/object/computed';
import { service } from '@ember-decorators/service';
import { action } from '@ember-decorators/object';

export default class Login extends Controller {
  /**
   * References the application session service
   * @type {Session}
   * @memberof Login
   */
  @service
  session: Session;

  /**
   * Banner alert service
   * @type {BannerService}
   * @memberof Login
   */
  @service
  banners: BannerService;

  /**
   * Aliases the name property on the component
   * @type {ComputedProperty<string>}
   * @memberof Login
   */
  @alias('name')
  username: string;

  /**
   * Aliases the password computed property on the component
   * @type {ComputedProperty<string>}
   * @memberof Login
   */
  @alias('pass')
  password: string;

  /**
   * On instantiation, error message reference is an empty string value
   * @type {string}
   * @memberof Login
   */
  errorMessage = '';

  @action
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
}
