import Route from '@ember/routing/route';
import { get } from '@ember/object';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { logout } from 'wherehows-web/utils/api/authentication';
import Session from 'ember-simple-auth/services/session';
import { action } from '@ember/object';
import { inject as service } from '@ember/service';

export default class Logout extends Route.extend(AuthenticatedRouteMixin) {
  /**
   * Reference to the application session service, implemented with Ember Simple Auth
   * @type {Session}
   */
  @service
  session: Session;

  /**
   * Post transition, call endpoint then invalidate current session on client on success
   */
  @action
  didTransition(this: Logout) {
    logout().then(() => get(this, 'session').invalidate());
  }
}
