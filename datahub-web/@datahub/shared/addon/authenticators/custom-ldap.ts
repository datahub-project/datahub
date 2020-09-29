import Base from 'ember-simple-auth/authenticators/base';
import { IAuthenticateResponse, IAuthenticationData } from '@datahub/shared/types/authentication/user';
import { postJSON } from '@datahub/utils/api/fetcher';

/**
 * Custom authenticator for ldap user authentication
 * @export
 * @class CustomLdap
 * @extends {Base}
 */
export default class CustomLdap extends Base {
  /**
   * Implements Base authenticator's authenticate method.
   *   Resolves with data object returned from successful request.
   * @param {string} username username to authenticate with
   * @param {string} password matching candidate password for username
   */
  async authenticate(username: string, password: string): Promise<IAuthenticationData> {
    const { data } = await postJSON<IAuthenticateResponse>({
      url: '/authenticate',
      data: { username, password }
    });

    return data;
  }

  /**
   * Implements the authenticator's restore method
   */
  restore(): Promise<void> {
    return Promise.resolve();
  }
}
