import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import fetch from 'ember-network/fetch';

const { get, Route, inject: { service } } = Ember;

export default Route.extend(AuthenticatedRouteMixin, {
  /**
   * @type {Ember.Service}
   */
  session: service(),

  actions: {
    /**
     * Post transition, call endpoint then invalidate current session on client on success
     */
    didTransition() {
      fetch('/logout').then(() => get(this, 'session').invalidate());
    }
  }
});
