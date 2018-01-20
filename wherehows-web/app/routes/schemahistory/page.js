import Route from '@ember/routing/route';

export default Route.extend({
  controllerName: 'schemahistory',

  setupController(controller, model) {
    controller.updateSchemas(model.page, 0);
  }
});
