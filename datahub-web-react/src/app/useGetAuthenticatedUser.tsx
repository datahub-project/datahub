import Cookies from 'js-cookie';
import { CLIENT_AUTH_COOKIE } from '../conf/Global';
import { useGetUserQuery } from '../graphql/user.generated';

/**
 * Fetch a CorpUser object corresponding to the currently authenticated user.
 */
export function useGetAuthenticatedUser() {
    const userUrn = Cookies.get(CLIENT_AUTH_COOKIE);
    if (!userUrn) {
        throw new Error('Could not find logged in user.');
    }
    const { data, error } = useGetUserQuery({ variables: { urn: userUrn } });
    if (error) {
        console.error(`Could not fetch logged in user from cache. + ${error.message}`);
    }
    return data?.corpUser;
}
