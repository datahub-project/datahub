import Route from '@ember/routing/route';
import { inject as service } from '@ember/service';
import CurrentUser from '@datahub/shared/services/current-user';

/**
 * Used to define the expected parameters for our route's model hook
 */
export interface IUserProfileRouteParams {
  // The username given for the route, begins the process of setting the context for us
  user_id: string;
}

/**
 * Used to define the return value for our route's model hook
 */
export interface IUserProfileRouteModel {
  // Given from the query param, will share the user's identifier to the route
  personUrn: string;
  // Temporary placeholder for configs that will be used to pouplate the tabs within the user
  // profile page
  pageConfigs: {};
}

/**
 * Note: After looking at multiple approaches, to a /user/me route, modifying this route to manually
 * check for a 'me' dynamic segment seems to be the best appraoch. It allows us to also check the dynamic
 * segment against a Urn class in the future. Alternatively, we could have made an actual separate
 * /user/me route.
 *
 * However, an issue that this ran into was that we could not further link to /user/me/<tabname> as
 * this caused the ember router to complain about additional dynamic segments. This means we would
 * have had to redefine the entire route structure for the user profile page under user/me which seems
 * to add more complexity than it solves.
 */
export default class UserProfile extends Route {
  /**
   * Injects the current user service to get the logged in user's information if we need to pass
   * along information for the the logged in user to the route model
   */
  @service
  currentUser!: CurrentUser;

  model({ user_id: identifier }: IUserProfileRouteParams): IUserProfileRouteModel {
    // If using the generic "me" identifier, then we route to the current user's profile page. This
    // helps us create a static path for the logged in user's profile page and makes elements on it
    // generically linkable
    const personUrn = identifier === 'me' ? (this.currentUser.entity?.urn as string) : identifier;

    return { personUrn, pageConfigs: {} };
  }
}
