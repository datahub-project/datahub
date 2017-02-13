import Ember from 'ember';

export default Ember.Route.extend({
  redirect() {
    this.transitionTo('datasets.page', 1);
  }
});
