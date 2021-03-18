import Cookies from 'js-cookie';
import { makeVar } from '@apollo/client';
import { GlobalCfg } from '../../conf';

export const checkAuthStatus = (): boolean => {
    return !!Cookies.get(GlobalCfg.CLIENT_AUTH_COOKIE);
};

export const isLoggedInVar = makeVar(checkAuthStatus());
