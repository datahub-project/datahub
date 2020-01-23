import Controller from '@ember/controller';
import { setProperties } from '@ember/object';
import Session from 'ember-simple-auth/services/session';
import { twoFABannerMessage, twoFABannerType } from '@datahub/utils/constants/notifications';
import BannerService from 'wherehows-web/services/banners';
import { alias } from '@ember/object/computed';
import { inject as service } from '@ember/service';
import { action } from '@ember/object';

export default class Login extends Controller {
  /**
   * References the application session service
   */
  @service
  session: Session;

  /**
   * Banner alert service
   */
  @service
  banners: BannerService;

  /**
   * Aliases the name property on the component
   */
  @alias('name')
  username: string;

  /**
   * Aliases the password computed property on the component
   */
  @alias('pass')
  password: string;

  /**
   * Aliases the VIP property on the component
   * This property is responsible for referencing the OTP token which is optionally required for user authentication
   */
  @alias('vip')
  vipToken: number;

  /**
   * On instantiation, error message reference is an empty string value
   */
  errorMessage = '';

  @action
  authenticateUser(): void {
    const { username, password, banners } = this;
    const vipToken = this.vipToken ? this.vipToken : '';
    this.session
      .authenticate('authenticator:custom-ldap', username, password ? password + vipToken : undefined)
      // Once user has chosen to authenticate, then we remove the login banner (if it exists) since it will
      // no longer be relevant
      .then(banners.removeBanner.bind(banners, twoFABannerMessage, twoFABannerType))
      .catch(({ responseText = 'Bad Credentials' }) => setProperties(this, { errorMessage: responseText }));
  }
}
