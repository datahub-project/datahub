import Controller from '@ember/controller';
import Session from 'ember-simple-auth/services/session';
import UserLookup from 'wherehows-web/services/user-lookup';
import Notifications from 'wherehows-web/services/notifications';
import BannerService from 'wherehows-web/services/banners';
import { service } from '@ember-decorators/service';

export default class Application extends Controller {
  /**
   * User session management service
   * @type {Session}
   */
  @service
  session: Session;

  /**
   * Looks up user names and properties from the partyEntities api
   * @type {UserLookup}
   */
  @service('user-lookup')
  ldapUsers: UserLookup;

  /**
   * References the application notifications service
   * @type {Notifications}
   */
  @service
  notifications: Notifications;

  /**
   * Adds the service for banners in order to trigger the application to render the banners when
   * they are triggered
   * @type {BannerService}
   */
  @service('banners')
  banners: BannerService;

  constructor() {
    super(...arguments);

    this.ldapUsers.fetchUserNames();
  }
}
