import React from 'react';
import { useHistory } from 'react-router';
import Cookies from 'js-cookie';
import analytics, { EventType } from '../analytics';
import { isLoggedInVar } from './checkAuthStatus';
import { GlobalCfg } from '../../conf';

export const LogOut = () => {
    const history = useHistory();
    const handleLogout = async () => {
        analytics.event({ type: EventType.LogOutEvent });
        isLoggedInVar(false);
        Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
        const logoutBackend = await fetch('/logOut');
        if (logoutBackend?.status === 200) {
            history.push('/');
        }
    };

    handleLogout();

    return <></>;
};
