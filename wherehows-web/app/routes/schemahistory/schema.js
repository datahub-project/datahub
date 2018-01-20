import Route from '@ember/routing/route';

export default Route.extend({
  setupController(controller, model) {
    this.controllerFor('schemahistory').schemahistory.updateSchemas(1, model.id);
  }
});
