import Ember from 'ember';

export default Ember.Route.extend({
  setupController(controller, model) {
    this.controllerFor('schemahistory').schemahistory.updateSchemas(1, model.id);
  }
});