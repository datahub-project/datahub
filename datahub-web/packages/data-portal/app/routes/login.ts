import Route from '@ember/routing/route';
import { get } from '@ember/object';
import { run } from '@ember/runloop';
import { inject as service } from '@ember/service';
import { twoFABannerMessage, twoFABannerType } from '@datahub/utils/constants/notifications';
import { getConfig } from 'datahub-web/services/configurator';
import BannerService from 'datahub-web/services/banners';
import Session from 'ember-simple-auth/services/session';
import { AuthenticationType } from '@datahub/shared/constants/authentication/auth-type';
import AadSso from '@datahub/shared/services/aad-sso';

export default class LoginRoute extends Route {
  @service
  session: Session;

  /**
   * Banner alert service
   * @type {Ember.Service}
   */
  @service
  banners: BannerService;

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
        // not authenticated and that is fine
      }
    }
  }

  async model(): Promise<{ isInternal: boolean | void; loginType: AuthenticationType }> {
    const isInternal = await getConfig('isInternal');
    const loginType = getConfig('authenticationWorkflow');

    return { isInternal, loginType };
  }

  /**
   * Overrides the default method with a custom op
   * renders the default template into the login outlet
   * @override
   */
  renderTemplate(): void {
    const loginType = getConfig('authenticationWorkflow');

    this.render(this.routeName, {
      outlet: 'login'
    });

    const { isInternal } = get(this, 'controller').get('model');

    if (loginType === AuthenticationType.Ldap && isInternal) {
      run.scheduleOnce('afterRender', this, 'showTwoFABanner');
    }
  }

  /**
   * After the login route has finished rendering, we can trigger the service to add a banner for the two
   * factor authentication.
   */
  showTwoFABanner(): void {
    const { banners } = this;
    banners.addBanner(twoFABannerMessage, twoFABannerType);
  }
}
