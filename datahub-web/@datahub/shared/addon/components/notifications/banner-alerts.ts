import Component from '@glimmer/component';
import { get, set } from '@ember/object';
import BannerService, { IBanner } from '@datahub/shared/services/banners';
import { bannerAnimationSpeed } from '@datahub/utils/constants/notifications';
import { inject as service } from '@ember/service';
import { alias } from '@ember/object/computed';
import { action } from '@ember/object';

export default class BannerAlerts extends Component {
  /**
   * Keeps the base banner animation speed as a property to attach as a data attribute to the rendered
   * banner-alert elements in the template
   */
  bannerAnimationSpeed = bannerAnimationSpeed;

  /**
   * Imports the service used to handle actual activation and dismissal of banners. The service also
   * maintains the banners list
   * @type {ComputedProperty<BannerService>}
   */
  @service
  banners!: BannerService;

  /**
   * References the banners service computation on whether or not we should be showing banners
   */
  @alias('banners.isShowingBanners')
  isShowingBanners!: boolean;

  /**
   * Triggered by the user by clicking the dismiss icon on the banner, triggers the exiting state on the
   * topmost (first in queue) banner and starts the timer/css animation for the dismissal action
   * @param {IBanner} banner - the banner as a subject for the dismissal action
   */
  @action
  onDismissBanner(this: BannerAlerts, banner: IBanner): void {
    const banners = get(this, 'banners');

    set(banner, 'isExiting', true);
    banners.dequeue();
  }
}
