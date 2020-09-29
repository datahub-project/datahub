import { inject as service } from '@ember/service';
import { twoFABannerMessage, twoFABannerType } from '@datahub/utils/constants/notifications';
import BannerService from '@datahub/shared/services/banners';
import LoginBaseRoute from '@datahub/shared/routes/login-base';

export default class LoginRoute extends LoginBaseRoute {
  /**
   * Banner alert service
   * @type {Ember.Service}
   */
  @service
  banners: BannerService;

  afterRender(): void {
    this.showTwoFABanner();
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
