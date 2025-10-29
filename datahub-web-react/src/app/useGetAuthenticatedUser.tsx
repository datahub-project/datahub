import Cookies from 'js-cookie';

import { CLIENT_AUTH_COOKIE } from '@conf/Global';

import { useGetMeQuery } from '@graphql/me.generated';

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
