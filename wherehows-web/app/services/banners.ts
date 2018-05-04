import Service from '@ember/service';
import { computed, get } from '@ember/object';
import { bannerAnimationSpeed } from 'wherehows-web/constants/notifications';
import { NotificationEvent } from 'wherehows-web/services/notifications';
import { delay } from 'wherehows-web/utils/promise-delay';

/**
 * Expected properties to be found on a basic banner object added to our list
 */
export interface IBanner {
  content: string;
  type: NotificationEvent;
  isExiting: boolean;
  isDismissable: boolean;
  iconName: string;
  usePartial: boolean;
}

/**
 * When creating a new banner, helps to determine whether that banner isDismissable
 */
const isDismissableMap: { [key: string]: boolean } = {
  [NotificationEvent['info']]: true,
  [NotificationEvent['confirm']]: true,
  [NotificationEvent['error']]: false
};

/**
 * When creating a new banner, helps to determine the kind of font awesome icon indicator will be on
 * the left side of the banner
 */
const iconNameMap: { [key: string]: string } = {
  [NotificationEvent['info']]: 'info-circle',
  [NotificationEvent['confirm']]: 'exclamation-circle',
  [NotificationEvent['error']]: 'times-circle'
};

export default class BannerService extends Service {
  /**
   * Our cached list of banner objects to be rendered
   * @type {Array<IBanner>}
   */
  banners: Array<IBanner> = [];

  /**
   * Computes the banners list elements to toggle the isShowingBanners flag, which not only activates the
   * banner component but also triggers the navbar and app-container body to shift up/down to accommodate
   * for the banner component space
   * @type {ComputedProperty<boolean>}
   */
  isShowingBanners = computed('banners.@each.isExiting', function() {
    const banners = get(this, 'banners');
    // Note: If we have no banners, flag should always be false. If we have more than one banner, flag
    // should always be true, BUT if we only have one banner and it is in an exiting state we can go ahead
    // and trigger this to be false to line up with the animation
    return banners.length > 0 && (banners.length > 1 || !banners[0].isExiting);
  });

  appInitialBanners([showStagingBanner, showStaleSearchBanner]: Array<boolean>): void {
    if (showStagingBanner) {
      this.addBanner(
        'You are viewing/editing in the staging environment. Changes made here will not reflect in production',
        NotificationEvent['info']
      );
    }

    if (showStaleSearchBanner) {
      this.addBanner('components/notifications/partials/stale-search-alert', NotificationEvent['info'], true);
    }
  }

  /**
   * Method to actually take care of removing the first banner from our queue.
   */
  async dequeue(): Promise<void> {
    // Note: Since dequeuing the banner will remove it from the DOM, we don't want to actually dequeue
    // until the removal animation, which takes 0.6 seconds, is completed.
    const animationOffset = bannerAnimationSpeed + 0.2;
    const dismissDelay = delay(animationOffset);
    const banners = get(this, 'banners');

    await dismissDelay;
    banners.removeAt(0, 1);
  }

  /**
   * Method to add the banner to our queue. Takes the content and type of banner and creates a
   * standardized interface that our servie can understand
   * @param message - the message to put in the banner's content box
   * @param {NotificationEvent} type - what type of banner notification we are going for (which will
   *  determine the appearance and interaction properties)
   * @param usePartial - in some situations, we want to use a partial component to render in the banner instead
   *  of a simple string. This flag will allow that
   */
  addBanner(message: string, type: NotificationEvent = NotificationEvent['info'], usePartial = false): void {
    get(this, 'banners').addObject({
      type,
      usePartial,
      content: message,
      isExiting: false,
      isDismissable: isDismissableMap[type],
      iconName: iconNameMap[type]
    });
  }
}
