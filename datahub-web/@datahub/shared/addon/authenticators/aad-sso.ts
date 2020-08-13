import Base from 'ember-simple-auth/authenticators/base';
import { inject as service } from '@ember/service';
import AadSso from '@datahub/shared/services/aad-sso';

/**
 * Ember Simple Auth Azure Active Directory Single Sign On authenticator
 */
export default class AadSsoAuthenticator extends Base {
  /**
   * Azure Active Directory Single Sign On service to share state across the app
   */
  @service('aad-sso')
  sso!: AadSso;

  /**
   * Will be invoked when launching the app (when browser loads the app) to restore previous sessions.
   * We will invoke the midtier to validate the tokens
   */
  restore(_data: string): Promise<void> {
    return this.sso.validateToken();
  }

  /**
   * Will authenticate if needed. By default will redirect to AAD to authenticate. If redirectToAAD is false
   * will not redirect which is useful when you are already logged in in the midtier but not in the frontend.
   * @param redirectToAAD
   */
  authenticate(redirectToAAD = true): Promise<void> {
    return this.sso.authenticate(redirectToAAD);
  }

  /**
   * Will destroy session data.
   * @param _data
   */
  invalidate(_data: string): Promise<void> {
    return Promise.resolve();
  }
}
