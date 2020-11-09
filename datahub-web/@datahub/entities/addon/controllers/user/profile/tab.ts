import Controller from '@ember/controller';
import { tracked } from '@glimmer/tracking';

/**
 * The UserProfileTabController is meant to handle the query parameters common to the tab routes
 * for the user profile page content items.
 */
export default class UserProfileTabController extends Controller {
  /**
   * Declares the query parameters for the associated route to this controller
   */
  queryParams = ['page'];

  /**
   * Keeps `@track` of the current pagination item in the route, used by any parameter that
   * requires pagination
   */
  @tracked
  page = 1;

  /**
   * Resets the query parameter data in the controller so that state does not leak between route
   * transitions
   */
  resetData(): void {
    this.page = 1;
  }
}

declare module '@ember/controller' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    'user/profile/tab': UserProfileTabController;
  }
}
