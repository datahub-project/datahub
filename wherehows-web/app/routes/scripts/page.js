import Route from '@ember/routing/route';

export default Route.extend({
  setupController(controller, model) {
    this.controllerFor('scripts')
      .get('updateScripts')
      .call(this.controllerFor('scripts'), model.page);
  }
});
