import Ember from 'ember';

export default Ember.Route.extend({
  redirect() {
    this.transitionTo('flows.flowspage', 1);
  }
});
