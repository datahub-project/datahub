import { setProperties } from '@ember/object';
import { twoFABannerMessage, twoFABannerType } from '@datahub/utils/constants/notifications';
import BannerService from '@datahub/shared/services/banners';
import { inject as service } from '@ember/service';
import { action } from '@ember/object';
import LoginBaseController from '@datahub/shared/controllers/login-base';

export default class Login extends LoginBaseController {
  /**
   * Banner alert service
   */
  @service
  banners: BannerService;

  /**
   * Handle for post authentication for any authentication method
   * @param authentication
   */
  @action
  async handlePostAuthentication(authentication: Promise<void>): Promise<void> {
    try {
      await authentication;
      // Once user has chosen to authenticate, then we remove the login banner (if it exists) since it will
      // no longer be relevant
      this.banners.removeBanner(twoFABannerMessage, twoFABannerType);
    } catch ({ message = 'Sorry, a login error occurred. Please reach out to support for a resolution' }) {
      setProperties(this, { errorMessage: message });
    }
  }
}
