import Controller from '@ember/controller';
import { computed, get, setProperties } from '@ember/object';
import { inject } from '@ember/service';

export default Controller.extend({
  session: inject(),

  username: computed.alias('name'),

  password: computed.alias('pass'),

  errorMessage: '',

  actions: {
    /**
     * Using the session service, authenticate using the custom ldap authenticator
     */
    authenticateUser() {
      const { username, password } = this.getProperties(['username', 'password']);

      get(this, 'session')
        .authenticate('authenticator:custom-ldap', username, password)
        .catch(({ responseText = 'Bad Credentials' }) => setProperties(this, { errorMessage: responseText }));
    }
  }
});
