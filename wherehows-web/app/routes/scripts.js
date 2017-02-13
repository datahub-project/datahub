import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Ember.Route.extend(AuthenticatedRouteMixin, {
  init() {
    this._super(...arguments);
    Ember.run.scheduleOnce('afterRender', this, 'bindKeyEvent');
  },

  bindKeyEvent() {
    $('.script-filter').bind("paste keyup", () => this.controller.updateScripts(1));
  }
});
