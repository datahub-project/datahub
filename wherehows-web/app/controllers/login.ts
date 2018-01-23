import Controller from '@ember/controller';
import { computed, get, setProperties, getProperties } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import { inject } from '@ember/service';
import Session from 'ember-simple-auth/services/session';

export default class Login extends Controller {
  /**
   * References the application session service
   * @type {ComputedProperty<Session>}
   * @memberof Login
   */
  session: ComputedProperty<Session> = inject();

  /**
   * Aliases the name property on the component
   * @type {ComputedProperty<string>}
   * @memberof Login
   */
  username = computed.alias('name');

  /**
   * Aliases the password computed property on the component
   * @type {ComputedProperty<string>}
   * @memberof Login
   */
  password = computed.alias('pass');

  /**
   * On instantiation, error message reference is an empty string value
   * @type {string}
   * @memberof Login
   */
  errorMessage = '';

  actions = {
    /**
     * Using the session service, authenticate using the custom ldap authenticator
     * @return {void}
     */
    authenticateUser(this: Login): void {
      const { username, password } = getProperties(this, ['username', 'password']);

      get(this, 'session')
        .authenticate('authenticator:custom-ldap', username, password)
        .catch(({ responseText = 'Bad Credentials' }) => setProperties(this, { errorMessage: responseText }));
    }
  };
}
