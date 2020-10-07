import Controller from '@ember/controller';
import { setProperties } from '@ember/object';
import Session from 'ember-simple-auth/services/session';
import { alias } from '@ember/object/computed';
import { inject as service } from '@ember/service';
import { action } from '@ember/object';
import { AppName } from '@datahub/shared/constants/global';
import { AuthenticationType } from '@datahub/shared/constants/authentication/auth-type';

export default class LoginBaseController extends Controller {
  /**
   * References the application session service
   */
  @service
  session!: Session;

  /**
   * Aliases the name property on the component
   */
  @alias('name')
  username?: string;

  /**
   * Aliases the password computed property on the component
   */
  @alias('pass')
  password?: string;

  /**
   * Aliases the VIP property on the component
   * This property is responsible for referencing the OTP token which is optionally required for user authentication
   */
  @alias('vip')
  vipToken?: number;

  AuthenticationType = AuthenticationType;

  /**
   * On instantiation, error message reference is an empty string value
   */
  errorMessage = '';

  /**
   * Name of the app being displayed on the Login page.
   */
  appName: string = AppName;

  /**
   * Authenticates the user credentials on the session service using the specified authenticator. Also handles authentication failures by updating the
   * errorMessage attribute on the controller instance
   */
  @action
  authenticateUser(authType: AuthenticationType = AuthenticationType.Ldap): Promise<void> {
    switch (authType) {
      case AuthenticationType.Ldap:
        return this.authViaLdap();

      case AuthenticationType.Sso:
        return this.authViaSso();
    }
  }

  /**
   * Handle for post authentication for any authentication method
   * @param authentication
   */
  @action
  async handlePostAuthentication(authentication: Promise<void>): Promise<void> {
    try {
      await authentication;
    } catch ({ message = 'Sorry, a login error occurred. Please reach out to support for a resolution' }) {
      setProperties(this, { errorMessage: message });
    }
  }

  /**
   * Will log in using LDAP
   */
  private authViaLdap(): Promise<void> {
    const { username, password } = this;
    const vipToken = this.vipToken ? this.vipToken : '';

    return this.handlePostAuthentication(
      this.session.authenticate('authenticator:custom-ldap', username, password ? password + vipToken : undefined)
    );
  }

  /**
   * Will log in using Azure Active Directory Single Sign On
   */
  private authViaSso(): Promise<void> {
    return this.handlePostAuthentication(this.session.authenticate('authenticator:aad-sso'));
  }
}
