import Base from 'ember-simple-auth/authenticators/base';
import Ember from 'ember';

const { $: { post } } = Ember;

export default Base.extend({
  /**
   * Implements Base authenticator's authenticate method.
   *   Resolves with data object returned from successful request.
   * @param {String} username username to authenticate with
   * @param {String} password matching candidate password for username
   * @return {Promise}
   */
  authenticate: (username, password) =>
    Promise.resolve(
      post({
        url: '/authenticate',
        contentType: 'application/json',
        data: JSON.stringify({ username, password })
      }).then(({ data }) => Object.assign({}, data))
    ),

  restore() {
    return Promise.resolve();
  }

  // TODO: Remove request server invalidate session
  // as unfortunately server is stateful and will retain an open session
  // invalidate() {
  // return Promise.resolve();
  // }
});
