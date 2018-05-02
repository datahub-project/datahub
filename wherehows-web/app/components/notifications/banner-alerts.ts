import Component from '@ember/component';
import { inject as service } from '@ember/service';
import { computed, get, set } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import BannerService, { IBanner } from 'wherehows-web/services/banners';
import { bannerAnimationSpeed } from 'wherehows-web/constants/notifications';

export default class BannerAlerts extends Component {
  /**
   * Imports the service used to handle actual activation and dismissal of banners. The service also
   * maintains the banners list
   * @type {Serivce}
   */
  banners: ComputedProperty<BannerService> = service();

  /**
   * Sets the tagname for the html element rendered by this component
   */
  tagName = 'section';

  /**
   * Sets the classnames to attach to the html element rendered by this component
   */
  classNames = ['banner-alerts'];

  /**
   * Keeps the base banner animation speed as a property to attach as a data attribute to the rendered
   * banner-alert elements in the template
   */
  bannerAnimationSpeed = bannerAnimationSpeed;

  /**
   * Binds classnames to specific truthy values of properties on this component
   */
  classNameBindings = ['isShowingBanners:banner-alerts--show:banner-alerts--hide'];

  /**
   * References the banners service computation on whether or not we should be showing banners
   */
  isShowingBanners: ComputedProperty<boolean> = computed.alias('banners.isShowingBanners');

  actions = {
    /**
     * Triggered by the user by clicking the dismiss icon on the banner, triggers the exiting state on the
     * topmost (first in queue) banner and starts the timer/css animation for the dismissal action
     * @param this - explicit this declaration for typescript
     * @param {IBanner} banner - the banner as a subject for the dismissal action
     */
    onDismissBanner(this: BannerAlerts, banner: IBanner) {
      const banners = get(this, 'banners');

      set(banner, 'isExiting', true);
      banners.dequeue();
    }
  };
}
