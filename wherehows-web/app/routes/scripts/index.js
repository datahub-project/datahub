import Ember from 'ember';

export default Ember.Route.extend({
  redirect() {
    this.transitionTo('scripts.page', 1);
  }
});
