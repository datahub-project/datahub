import Ember from 'ember';

const {
  Controller,
  computed,
  setProperties,
  inject: {service}
} = Ember;

export default Controller.extend({
  session: service(),

  username: computed.alias('name'),

  password: computed.alias('pass'),

  errorMessage: '',

  actions: {
    /**
     * Using the session service, authenticate using the custom ldap authenticator
     */
    authenticateUser() {
      const {username, password} = this.getProperties([
        'username',
        'password'
      ]);

      this.get('session')
        .authenticate('authenticator:custom-ldap', username, password)
        .catch(({responseText = 'Bad Credentials'}) =>
          setProperties(this, {errorMessage: responseText}));
    }
  }
});
