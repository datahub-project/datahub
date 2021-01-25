import Cookies from 'js-cookie';
import { makeVar } from '@apollo/client';

export const checkAuthStatus = (): boolean => {
    // Check if we have a valid token.
    // TODO: perhaps there's a more robust way to detect this?
    // e.g. what happens if the PLAY_SESSION cookie is stuck but the session is
    // invalid or expired?
    return !!Cookies.get('PLAY_SESSION');
};

export const isLoggedInVar = makeVar(checkAuthStatus());
