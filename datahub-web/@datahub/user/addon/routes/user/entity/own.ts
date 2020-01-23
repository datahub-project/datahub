import Route from '@ember/routing/route';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import { IUserEntityRouteModel } from '@datahub/user/routes/user/entity';
import { inject } from '@ember/service';
import CurrentUser from '@datahub/shared/services/current-user';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/entity/rendering/search-entity-render-prop';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

/**
 * Query params required to maintain search state in the url
 */
interface IUserEntityOwnQueryParams {
  page?: string;
  facets?: string;
}

/**
 * Model for the route. It will contain the fields that are needed for
 * the search result, facets and page for query params search state and userName for
 * ownership query
 */
interface IUserEntityOwnModel extends IUserEntityRouteModel {
  userName: string;
  page: number;
  facets?: string;
  fields: Array<ISearchEntityRenderProps>;
}

/**
 * Entity I Own page. It will show similar search result page with the entities I own
 */
export default class UserEntityOwn extends Route {
  queryParams = refreshModelForQueryParams(['page', 'facets']);

  /**
   * @type {CurrentUser}
   */
  @inject('current-user')
  sessionUser!: CurrentUser;

  /**
   * Will expose entity from parent route, search query parameters (facets, page) so
   * search container can use it, current user name to fill ownership query and fields to use.
   *
   * it will try to use `userEntityOwnership` if found, falling back to `search` fields.
   */
  model({ page, facets }: IUserEntityOwnQueryParams): IUserEntityOwnModel {
    const { entity } = this.modelFor('user.entity') as IUserEntityRouteModel;
    const { renderProps } = DataModelEntity[entity];
    const { userEntityOwnership, search } = renderProps;
    const { currentUser } = this.sessionUser;

    return {
      entity,
      userName: currentUser ? currentUser.userName : '',
      page: Number(page || 1),
      facets,
      fields: userEntityOwnership && userEntityOwnership.attributes ? userEntityOwnership.attributes : search.attributes
    };
  }
}
