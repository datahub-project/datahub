import Route from '@ember/routing/route';

export default Route.extend({
  redirect: function() {
    this.transitionTo('metadata.user', 'jweiner');
  }
});
