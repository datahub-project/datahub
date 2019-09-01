import Service from '@ember/service';
import { IBanner } from 'wherehows-web/services/banners';

/**
 * Stubs out the Banner Ember Service for tests or components
 * that are dependent on the application service
 * @export
 * @class BannerServiceStub
 * @extends {Service}
 */
export default class BannerServiceStub extends Service {
  banners: Array<IBanner> = [];
  isShowingBanners: boolean = false;
}
