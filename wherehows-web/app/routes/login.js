import Ember from 'ember';

const { Route, get, inject: { service } } = Ember;

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
  },

  /**
   * Overrides the default method with a custom op
   * renders the default template into the login outlet
   * @override
   */
  renderTemplate() {
    this.render({
      outlet: 'login'
    });
  }
});
