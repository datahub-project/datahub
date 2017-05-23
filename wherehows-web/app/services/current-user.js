import Ember from 'ember';

const { get, set, isBlank, $: { getJSON }, inject: { service }, Service } = Ember;
const currentUserUrl = '/api/v1/user/me';

export default Service.extend({
  session: service(),

  /**
   * Attempt to load the currently logged in user.
   *   If the userName is found from a previously retained session,
   *   append to service. Request the full user object, and append
   *   to service.
   * @returns {Promise}
   */
  load() {
    const userName = get(this, 'session.data.authenticated.username');

    if (!isBlank(userName)) {
      set(this, 'userName', userName);
    }

    // If we have a valid session, get the currently logged in user, and set the currentUser attribute,
    // otherwise raise an exception
    if (get(this, 'session.isAuthenticated')) {
      return Promise.resolve(getJSON(currentUserUrl)).then(
        ({ status = 'failed', user = {} }) =>
          (status === 'ok'
            ? Promise.resolve(set(this, 'currentUser', user))
            : Promise.reject(new Error(`Load current user failed with status: ${status}`)))
      );
    }

    return Promise.resolve();
  },

  /**
   * Invalidates the current session if the session is currently valid
   * useful if, for example, the server is no able to provide the currently logged in user
   * @return {Boolean|*|Ember.RSVP.Promise}
   */
  invalidateSession() {
    const sessionService = get(this, 'session');
    return sessionService.isAuthenticated && sessionService.invalidate();
  }
});
