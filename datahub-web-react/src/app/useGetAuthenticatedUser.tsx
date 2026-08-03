import Cookies from 'js-cookie';

import { useUserContext } from '@app/context/useUserContext';
import { CLIENT_AUTH_COOKIE } from '@conf/Global';

import { GetMeQuery } from '@graphql/me.generated';

type AuthenticatedUser = NonNullable<GetMeQuery['me']>;

/**
 * Authenticated user details from {@link UserContextProvider}.
 *
 * Prefer {@link useUserContext} for new call sites. This hook remains for
 * existing consumers that expect the historical `{ corpUser, platformPrivileges }` shape
 * from `getMe`, without issuing a second Apollo `getMe` query.
 */
export function useGetAuthenticatedUser(): AuthenticatedUser | undefined {
    const { loaded, user, platformPrivileges } = useUserContext();

    if (!loaded || !user) {
        return undefined;
    }

    // UserContext stores the same getMe payload; cast back to the query shape so
    // consumers retain fields aliased on corpUser when present.
    return {
        corpUser: user as AuthenticatedUser['corpUser'],
        platformPrivileges: (platformPrivileges ?? undefined) as AuthenticatedUser['platformPrivileges'],
    };
}

/**
 * Return a list of URN corresponding to the currently authenticated user, null if one cannot be found.
 */
export function useGetAuthenticatedUserUrn() {
    const userUrn = Cookies.get(CLIENT_AUTH_COOKIE);
    if (!userUrn) {
        throw new Error('Could not find logged in user.');
    }
    return userUrn;
}
