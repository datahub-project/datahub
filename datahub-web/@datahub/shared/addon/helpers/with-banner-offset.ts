import Helper from '@ember/component/helper';
import { inject as service } from '@ember/service';
import BannerService from '@datahub/shared/services/banners';
import { alias } from '@ember/object/computed';
import { observes } from '@ember-decorators/object';

// TODO [META-12344] Move this to shared
/**
 * Stateful helper toggles off or on a provided / built-in modifier class name which offsets for baseClass with
 * values that respect the rendered banners
 * @export
 * @class WithBannerOffset
 * @extends {Helper}
 */
export default class WithBannerOffset extends Helper {
  /**
   * References the application Banner service
   * @type {BannerService}
   * @memberof WithBannerOffset
   */
  @service
  banners!: BannerService;

  /**
   * Aliases the flag on the banner service which indicates if the viewport has banners visible
   * @type {boolean}
   * @memberof WithBannerOffset
   */
  @alias('banners.isShowingBanners')
  shouldOffsetBanners?: boolean;

  /**
   * Resolves the appropriate class name to offset the visible banners
   * @param {Array<string>} [baseClass, alternateOffsetClass] baseClass is the primary class for the element that contains the base style rules
   * by default, if only the baseClass is supplied, when banners.isShowing is true, the baseClass is concatenated with a BEM modifier
   * if an alternateOffsetClass is passed in, that class will be used if the banners are visible
   * e.g
   * if offsetting, {{with-banner-offset "my-base-class"}} => "my-base-class my-base-class--with-banner-offset"
   * if offsetting with alternate, {{with-banner-offset "my-base-class" "my-alternate-offset-class"}} => "my-base-class my-alternate-offset-class"
   *
   * if not offsetting, {{with-banner-offset "my-base-class"}} => "my-base-class"
   * @returns {string}
   * @memberof WithBannerOffset
   */
  compute([baseClass, alternateOffsetClass]: Array<string>): string {
    const baseClassWithOffsetModifier = alternateOffsetClass
      ? `${baseClass} ${alternateOffsetClass}`
      : `${baseClass} ${baseClass}--with-banner-offset`;

    return this.shouldOffsetBanners ? baseClassWithOffsetModifier : baseClass;
  }

  /**
   * When banner.isShowingBanners updates, recompute the offset by toggling class on / off
   * unfortunately no way for stateful helpers to recompute state without an observer
   * @link https://api.emberjs.com/ember/3.8/classes/Helper/methods/recompute?anchor=recompute
   * @link https://guides.emberjs.com/release/templates/writing-helpers/#toc_class-based-helpers
   */
  @observes('banners.isShowingBanners') //eslint-disable-line ember/no-observers
  toggleOffsetClassIfNeeded(): void {
    this.recompute();
  }
}
