import Service from '@ember/service';
import { ApiResponseStatus } from '@datahub/utils/api/shared';
import { action } from '@ember/object';
import { inject as service } from '@ember/service';
import Session from 'ember-simple-auth/services/session';
import { EmberCookieService } from 'ember-cookies';
/**
 * SSO Service for UI for ember simple auth
 */
export default class AadSso extends Service {
  /**
   * Session Service to check the redirect logic after login
   */
  @service
  session!: Session;

  /**
   * Ember Cookies service to write 'ember_simple_auth-redirectTarget'
   * that will be used to redirect after a successful login (MSAL changes the page)
   */
  @service
  cookies!: EmberCookieService;

  /**
   * API endpoint for login
   */
  authRedirectUrl = '/sso';

  /**
   * API endpoint to check auth status
   */
  validateTokenUrl = '/validate-token';

  /**
   * Will invoke validate token. If API returns 401 means that the user is not authorized.
   */
  @action
  async validateToken(): Promise<void> {
    const { status } = await fetch(this.validateTokenUrl);
    if (status === ApiResponseStatus.UnAuthorized) {
      throw new Error(`${ApiResponseStatus.UnAuthorized}`);
    }
    return;
  }

  /**
   * Will authenticate by just validating the token. If the user is not authenticated, it will redirected to
   * MSAL login page by default.
   * @param redirectToAAD
   */
  async authenticate(redirectToAAD = true): Promise<void> {
    try {
      return await this.validateToken();
    } catch {
      if (redirectToAAD) {
        const { authRedirectUrl } = this;
        if (this.session.attemptedTransition) {
          const transition = (this.session.attemptedTransition as unknown) as { intent: { url: string } };
          this.cookies.write('ember_simple_auth-redirectTarget', transition.intent.url);
        }
        window.location.replace(authRedirectUrl);
      }
      throw new Error('Not Authenticated');
    }
  }
  /**
   * Will check if we have the redirect cookie for ember-simple-auth
   */
  hasRedirectCookie(): boolean {
    return !!this.cookies.read('ember_simple_auth-redirectTarget');
  }
}

declare module '@ember/service' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    'aad-sso': AadSso;
  }
}
