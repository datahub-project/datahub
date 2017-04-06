import Ember from 'ember';

const {
  get,
  set,
  isBlank,
  $: { getJSON },
  inject: { service },
  Service
} = Ember;
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

    if (get(this, 'session.isAuthenticated')) {
      return Promise.resolve(getJSON(currentUserUrl)).then(({
        status = 'error',
        user = {}
      }) => {
        if (status === 'ok') {
          return Promise.resolve(set(this, 'currentUser', user));
        }

        return Promise.reject(
          new Error(`Load current user failed with status: ${status}`)
        );
      });
    }

    return Promise.resolve();
  }
});
