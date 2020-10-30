import { getRequest } from '@datahub/utils/api/fetcher';
const logoutUrl = '/logout';

/**
 * Calls the logout endpoint to log out the currently logged in user
 * @return {Promise<Response>}
 */
export const logout = (): Promise<Response> => getRequest({ url: logoutUrl });
