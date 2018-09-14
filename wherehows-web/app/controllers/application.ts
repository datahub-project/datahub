import Controller from '@ember/controller';
import Session from 'ember-simple-auth/services/session';
import Search from 'wherehows-web/services/search';
import UserLookup from 'wherehows-web/services/user-lookup';
import Notifications from 'wherehows-web/services/notifications';
import BannerService from 'wherehows-web/services/banners';
import { action } from '@ember-decorators/object';
import { service } from '@ember-decorators/service';

export default class Application extends Controller {
  /**
   * User session management service
   * @type {Session}
   */
  @service
  session: Session;

  /**
   * Injected global search service
   * @type {Search}
   */
  @service
  search: Search;

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

  keyword: string;

  constructor() {
    super(...arguments);

    this.ldapUsers.fetchUserNames();
  }

  /**
   * Invokes the search service api to transition to the
   *   search results page with the search parameters
   * @param {String} [keyword] the search string to search for
   * @param {String} [category] restrict search to results found here
   */
  @action
  didSearch({ keyword, category }: { keyword: string; category: string }) {
    this.search.showSearchResults({ keyword, category });
  }
}
