import Ember from 'ember';

const {
  Component
} = Ember;

export default Component.extend({
  classNames: ['nacho-login-form'],
  actions: {
    /**
     * Handle the login for submission
     */
    userDidSubmit() {
      // Trigger action on parent controller
      this.get('onSubmit')();
    }
  }
})