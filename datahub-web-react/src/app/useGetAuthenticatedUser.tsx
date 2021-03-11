import Cookies from 'js-cookie';
import { CLIENT_AUTH_COOKIE } from '../conf/Global';
import { useGetUserQuery } from '../graphql/user.generated';

/**
 * Fetch a CorpUser object corresponding to the currently authenticated user.
 */
export function useGetAuthenticatedUser() {
    return useGetUserQuery({ variables: { urn: Cookies.get(CLIENT_AUTH_COOKIE) || '' } });
}
