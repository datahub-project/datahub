import Route from '@ember/routing/route';
import { get } from '@ember/object';
import { run } from '@ember/runloop';
import { inject as service } from '@ember/service';
import { twoFABannerMessage, twoFABannerType } from '@datahub/utils/constants/notifications';
import { getConfig } from 'wherehows-web/services/configurator';
import BannerService from 'wherehows-web/services/banners';
import Session from 'ember-simple-auth/services/session';

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
   * Check is the user is currently authenticated when attempting to access
   *   this route, if so transition to index
   */
  redirect(): void {
    const {
      session: { isAuthenticated }
    } = this;
    if (isAuthenticated) {
      this.transitionTo('index');
    }
  }

  async model(): Promise<{ isInternal: boolean | void }> {
    const isInternal = await getConfig('isInternal');

    return { isInternal };
  }

  /**
   * Overrides the default method with a custom op
   * renders the default template into the login outlet
   * @override
   */
  renderTemplate(): void {
    this.render(this.routeName, {
      outlet: 'login'
    });

    const { isInternal } = get(this, 'controller').get('model');

    if (isInternal) {
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
