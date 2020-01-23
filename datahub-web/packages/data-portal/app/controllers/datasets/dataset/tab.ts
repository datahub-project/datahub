import Controller from '@ember/controller';
import BannerService from 'wherehows-web/services/banners';
import { inject as service } from '@ember/service';
import CurrentUser from '@datahub/shared/services/current-user';
import { alias } from '@ember/object/computed';

/**
 * Defines the class for datasets/dataset/tab route controller
 * Provides access to application objects like the BannerService instance
 * to templates and components lower in the hierarchy
 * @export
 * @class DatasetTab
 * @extends {Controller}
 */
export default class DatasetTab extends Controller {
  /**
   * References the application banner service
   * @type {BannerService}
   * @memberof DatasetTab
   */
  @service
  banners: BannerService;

  /**
   * References the CurrentUser service
   */
  @service('current-user')
  currentUser: CurrentUser;

  /**
   * Aliases the ldap username of the currently logged in user
   */
  @alias('currentUser.currentUser.userName')
  userName: string;

  /**
   * If we have a relevant metric for the dataset, then its urn is stored here
   * @type {string}
   */
  metric_urn?: string;
}

declare module '@ember/controller' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    'datasets/dataset/tab': DatasetTab;
  }
}
