import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

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

export default class UserProfile extends Route.extend(AuthenticatedRouteMixin) {
  model({ user_id: personUrn }: IUserProfileRouteParams): IUserProfileRouteModel {
    return { personUrn, pageConfigs: {} };
  }
}
