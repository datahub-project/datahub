import Ember from 'ember';

const {
  Route,
  get,
  inject: { service }
} = Ember;

export default Route.extend({
  session: service(),

  /**
   * Check is the user is currently authenticated when attempting to access
   *   this route, if so transition to index
   */
  redirect() {
    if (get(this, 'session.isAuthenticated')) {
      this.transitionTo('index');
    }
  }
});
