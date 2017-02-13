import Ember from 'ember';

export default Ember.Route.extend({
  init() {
    this._super(...arguments);

    Ember.run.scheduleOnce('afterRender', null, () => {
      Ember.$('#jiratabs a:first').tab('show');
    });
  },

  redirect: function () {
    this.transitionTo('idpc.user', 'jweiner');
  }
});
