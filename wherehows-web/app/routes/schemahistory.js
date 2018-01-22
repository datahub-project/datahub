import Route from '@ember/routing/route';
import { run, scheduleOnce } from '@ember/runloop';
import $ from 'jquery';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Route.extend(AuthenticatedRouteMixin, {
  init() {
    this._super(...arguments);
    scheduleOnce('afterRender', this, 'bindKeyEvent');
  },

  bindKeyEvent() {
    $('#name').bind('paste keyup', () =>
      scheduleOnce(() => {
        this.controller.set('schemaName', $('#name').val());
        this.controller.updateSchemas(1, 0);
      })
    );
  }
});
