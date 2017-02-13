import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Ember.Route.extend(AuthenticatedRouteMixin, {
  init() {
    this._super(...arguments);

    Ember.run.scheduleOnce('afterRender', null, () => {
      Ember.$('#dashboardtabs a:first').tab('show');
    });
  },

  redirect() {
    this.transitionTo('datasets.page', 1);
  }
});
