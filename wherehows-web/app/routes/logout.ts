import Route from '@ember/routing/route';
import ComputedProperty from '@ember/object/computed';
import { get } from '@ember/object';
import { inject } from '@ember/service';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { logout } from 'wherehows-web/utils/api/authentication';
import Session from 'ember-simple-auth/services/session';

export default class Logout extends Route.extend(AuthenticatedRouteMixin) {
  /**
   * Reference to the application session service, implemented with Ember Simple Auth
   * @type {ComputedProperty<Session>}
   */
  session: ComputedProperty<Session> = inject();

  actions = {
    /**
     * Post transition, call endpoint then invalidate current session on client on success
     */
    didTransition(this: Logout) {
      logout().then(() => get(this, 'session').invalidate());
    }
  };
}
