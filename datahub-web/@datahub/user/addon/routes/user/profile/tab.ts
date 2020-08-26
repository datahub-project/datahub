import Route from '@ember/routing/route';
import { IUserProfileRouteModel } from '@datahub/user/routes/user/profile';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import Transition from '@ember/routing/-private/transition';
import UserProfileTabController from '@datahub/user/controllers/user/profile/tab';

/**
 * Describes the params received by this route
 */
interface IUserProfileTabRouteParams {
  // The tab_selected dynamic segment allows us to make each tab associated with a route, allowing
  // a user to reach a specific tab through the url
  // eslint-disable-next-line @typescript-eslint/camelcase
  tab_selected: string;
  // The current page number for tabs that deal with pagination
  page: number;
}

/**
 * The expected return interface for the model
 */
export interface IUserProfileTabModel extends IUserProfileRouteModel {
  // The tab_selected dynamic segment allows us to make each tab associated with a route, allowing
  // a user to reach a specific tab through the url
  tabSelected: string;
  // Object that captures the relevant query params for the various profile page tabs
  queryParams: { page: number };
}

export default class UserProfileTab extends Route {
  /**
   * Settings for query parameters for this route.
   */
  queryParams = refreshModelForQueryParams(['page']);

  /**
   * In addition to the basic profile information, this utilizes a dynamic segment to retrieve the
   * currently selected tab for the user profile page navigation
   * @param {string} tabSelected - the identifier for the currently selected tab
   * @param {number} page - page number for the current pagination item in the tab
   */
  // eslint-disable-next-line @typescript-eslint/camelcase
  model({ tab_selected: tabSelected, page }: IUserProfileTabRouteParams): IUserProfileTabModel {
    const userProfileModel = this.modelFor('user.profile') as IUserProfileRouteModel;

    if (!userProfileModel || !userProfileModel.personUrn) {
      throw new Error('A person urn must be provided from the user.profile route');
    }

    return {
      ...userProfileModel,
      tabSelected,
      queryParams: { page }
    };
  }

  /**
   * Will cleanup controller, so there are no variables leaking.
   */
  resetController(controller: UserProfileTabController, isExiting: boolean, transition: Transition): void {
    super.resetController(controller, isExiting, transition);

    if (isExiting) {
      controller.resetData();
    }
  }
}
