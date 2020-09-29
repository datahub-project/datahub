import Route from '@ember/routing/route';
import { inject as service } from '@ember/service';
import { getConfig } from '@datahub/shared/services/configurator';
import Session from 'ember-simple-auth/services/session';
import { AuthenticationType } from '@datahub/shared/constants/authentication/auth-type';
import AadSso from '@datahub/shared/services/aad-sso';

export default class LoginBaseRoute extends Route {
  @service
  session!: Session;

  /**
   * Inject the Azure Active Directory Single Sign On service
   */
  @service('aad-sso')
  sso!: AadSso;

  /**
   * If you land on login page and you are already authenticated, then go to the attempted transition.
   */
  async beforeModel(): Promise<void> {
    const loginType = getConfig('authenticationWorkflow');

    if (this.session.isAuthenticated) {
      // if it is already authenticated then redirect
      this.transitionTo('index');
      return;
    }

    if (!this.session.isAuthenticated && loginType === AuthenticationType.Sso) {
      try {
        // if already logged in
        await this.sso.validateToken();

        if (this.sso.hasRedirectCookie()) {
          // remove attempted transition to make ember-simple-auth read cookie
          this.session.attemptedTransition = null;
        }

        await this.session.authenticate('authenticator:aad-sso', false);

        if (this.session.attemptedTransition) {
          this.session.attemptedTransition.retry();
        }
      } catch {
        // Purposely omiting await as it will trigger an exception if not authenticated and will redirect to SSO page
        this.session.authenticate('authenticator:aad-sso', true);
      }
    }
  }

  async model(): Promise<{ isInternal: boolean | void; loginType: AuthenticationType }> {
    const isInternal = await getConfig('isInternal');
    const loginType = getConfig('authenticationWorkflow');

    return { isInternal, loginType };
  }

  // override if needed
  afterRender(): void {
    //nothing to do now
  }
}
