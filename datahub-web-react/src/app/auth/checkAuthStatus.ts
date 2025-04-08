import { makeVar } from '@apollo/client';
import Cookies from 'js-cookie';

import analytics from '@app/analytics';
import { GlobalCfg } from '@src/conf';

export const checkAuthStatus = (): boolean => {
    const isAuthenticated = !!Cookies.get(GlobalCfg.CLIENT_AUTH_COOKIE);
    if (isAuthenticated) {
        analytics.identify(Cookies.get(GlobalCfg.CLIENT_AUTH_COOKIE) as string);
    }
    return isAuthenticated;
};

export const isLoggedInVar = makeVar(checkAuthStatus());
