import Base from 'ember-simple-auth/authenticators/base';
import { IAuthenticateResponse, IAuthenticationData } from 'wherehows-web/typings/api/authentication/user';
import JQuery from 'jquery';

const { post } = JQuery;

export default Base.extend({
  /**
   * Implements Base authenticator's authenticate method.
   *   Resolves with data object returned from successful request.
   * @param {string} username username to authenticate with
   * @param {string} password matching candidate password for username
   * @return {Promise<IAuthenticationData>}
   */
  authenticate: async (username: string, password: string): Promise<IAuthenticationData> => {
    // retaining usage of jquery post method as opposed to fetch, since api currently fails with string response
    // TODO: update mid-tier exception handling
    const { data } = await (<JQuery.jqXHR<IAuthenticateResponse>>post({
      url: '/authenticate',
      contentType: 'application/json',
      data: JSON.stringify({ username, password })
    }));

    return { ...data };
  },

  /**
   * Implements the authenticator's restore method
   * @return {Promise<void>}
   */
  restore: () => Promise.resolve()
});
