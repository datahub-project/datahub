import Ember from 'ember';

export default Ember.Route.extend({
  redirect() {
    this.transitionTo('metrics.metricspage', 1);
  }
});
