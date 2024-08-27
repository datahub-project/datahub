import Cookies from 'js-cookie';
import { CLIENT_AUTH_COOKIE } from '../conf/Global';
import { useGetMeQuery } from '../graphql/me.generated';

/**
 * Fetch a CorpUser object corresponding to the currently authenticated user.
 */
export function useGetAuthenticatedUser(skip?: boolean) {
    const userUrn = Cookies.get(CLIENT_AUTH_COOKIE);
    const { data, error } = useGetMeQuery({ skip: skip || !userUrn, fetchPolicy: 'cache-and-network' });
    if (error) {
        console.error(`Could not fetch logged in user from cache. + ${error.message}`);
    }
    return data?.me;
}

/**
 * Fetch an urn corresponding to the authenticated user.
 */
export function useGetAuthenticatedUserUrn() {
    const userUrn = Cookies.get(CLIENT_AUTH_COOKIE);
    if (!userUrn) {
        throw new Error('Could not find logged in user.');
    }
    return userUrn;
}
