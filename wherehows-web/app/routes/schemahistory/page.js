import Ember from 'ember';

export default Ember.Route.extend({
  controllerName: 'schemahistory',

  setupController(controller, model) {
    controller.updateSchemas(model.page, 0);
  }
});