import Route from '@ember/routing/route';
import { scheduleOnce } from '@ember/runloop';
import $ from 'jquery';

export default Route.extend({
  init() {
    this._super(...arguments);

    scheduleOnce('afterRender', null, () => {
      $('#jiratabs a:first').tab('show');
    });
  },

  redirect() {
    this.transitionTo('idpc.user', 'jweiner');
  }
});
