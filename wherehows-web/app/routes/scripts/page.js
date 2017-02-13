import Ember from 'ember';

export default Ember.Route.extend({
  setupController(controller, model) {
    this.controllerFor('scripts').get('updateScripts').call(this.controllerFor('scripts'), model.page);
  }
});