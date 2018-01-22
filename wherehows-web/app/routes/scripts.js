import Route from '@ember/routing/route';
import { scheduleOnce } from '@ember/runloop';
import $ from 'jquery';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Route.extend(AuthenticatedRouteMixin, {
  init() {
    this._super(...arguments);
    scheduleOnce('afterRender', this, 'bindKeyEvent');
  },

  bindKeyEvent() {
    $('.script-filter').bind('paste keyup', () => this.controller.updateScripts(1));
  }
});
