import Route from '@ember/routing/route';
import { IUserProfileRouteModel } from '@datahub/user/routes/user/profile';

/**
 * Describes the params received by this route
 */
interface IUserProfileTabRouteParams {
  // The tab_selected dynamic segment allows us to make each tab associated with a route, allowing
  // a user to reach a specific tab through the url
  // eslint-disable-next-line @typescript-eslint/camelcase
  tab_selected: string;
}

/**
 * The expected return interface for the model
 */
export interface IUserProfileTabModel extends IUserProfileRouteModel {
  tabSelected: string;
}

export default class UserProfileTab extends Route {
  /**
   * In addition to the basic profile information, this utilizes a dynamic segment to retrieve the
   * currently selected tab for the user profile page navigation
   * @param {string} tabSelected - the identifier for the currently selected tab
   */
  // eslint-disable-next-line @typescript-eslint/camelcase
  model({ tab_selected: tabSelected }: IUserProfileTabRouteParams): IUserProfileTabModel {
    const userProfileModel = this.modelFor('user.profile') as IUserProfileRouteModel;

    if (!userProfileModel || !userProfileModel.personUrn) {
      throw new Error('A person urn must be provided from the user.profile route');
    }

    return {
      ...userProfileModel,
      tabSelected
    };
  }
}
