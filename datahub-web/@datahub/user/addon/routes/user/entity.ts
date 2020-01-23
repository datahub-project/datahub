import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

/**
 * Describes the params and return of the model of the route
 */
export interface IUserEntityRouteModel {
  // The alias of the data model entity type
  entity: DataModelEntity['displayName'];
}

/**
 * User menu for Entities. This is the parent route
 * for pages like Features I Own or Entity I Follow
 */
export default class UserEntity extends Route.extend(AuthenticatedRouteMixin) {
  model(params: IUserEntityRouteModel): IUserEntityRouteModel {
    return { ...params };
  }
}
