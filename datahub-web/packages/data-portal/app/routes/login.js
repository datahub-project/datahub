import Route from '@ember/routing/route';
import { get } from '@ember/object';
import { run } from '@ember/runloop';
import { inject as service } from '@ember/service';
import { twoFABannerMessage, twoFABannerType } from '@datahub/utils/constants/notifications';
import Configurator from 'wherehows-web/services/configurator';

export default Route.extend({
  session: service(),

  /**
   * Banner alert service
   * @type {Ember.Service}
   */
  banners: service(),

  /**
   * Check is the user is currently authenticated when attempting to access
   *   this route, if so transition to index
   */
  redirect() {
    if (get(this, 'session.isAuthenticated')) {
      this.transitionTo('index');
    }
  },

  async model() {
    const { getConfig } = Configurator;
    const isInternal = await getConfig('isInternal');

    return { isInternal };
  },

  /**
   * Overrides the default method with a custom op
   * renders the default template into the login outlet
   * @override
   */
  renderTemplate() {
    this.render({
      outlet: 'login'
    });

    const { isInternal } = get(this, 'controller').get('model');

    if (isInternal) {
      run.scheduleOnce('afterRender', this, 'showTwoFABanner');
    }
  },

  /**
   * After the login route has finished rendering, we can trigger the service to add a banner for the two
   * factor authentication.
   */
  showTwoFABanner() {
    const banners = get(this, 'banners');
    banners.addBanner(twoFABannerMessage, twoFABannerType);
  }
});
