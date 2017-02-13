import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Ember.Route.extend(AuthenticatedRouteMixin, {
  init() {
    this._super(...arguments);
    Ember.run.scheduleOnce('afterRender', this, 'bindKeyEvent');
  },

  bindKeyEvent() {
    Ember.$('#name').bind('paste keyup', () => {
      this.controller.set('schemaName', Ember.$("#name").val());
      this.controller.updateSchemas(1, 0);
    });
  }
});
