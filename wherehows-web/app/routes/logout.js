import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

const {Route, inject: {service}} = Ember;


export default Route.extend(AuthenticatedRouteMixin, {
  session: service(),

  actions: {
    didTransition() {
      Ember.$.get('/logout').then(() => {
      this.get('session').invalidate();
      });
    }
  }
})