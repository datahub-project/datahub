import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import CurrentUser from '@datahub/shared/services/current-user';
import { inject } from '@ember/service';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

/**
 * User Ump execution flows he/she owns.
 */
export default class UserUmpFlow extends Route.extend(AuthenticatedRouteMixin) {
  /**
   * @type {CurrentUser}
   */
  @inject('current-user')
  sessionUser: CurrentUser;

  /**
   * Returns the current user as an entity for the ump flows route, assumes that the route context
   * should be the logged in user
   */
  model(): { user?: PersonEntity } {
    return { user: this.sessionUser.entity };
  }
}
