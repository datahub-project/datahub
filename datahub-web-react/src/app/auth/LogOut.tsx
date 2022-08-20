import React from 'react';
import { Redirect } from 'react-router';
import Cookies from 'js-cookie';
import analytics, { EventType } from '../analytics';
import { isLoggedInVar } from './checkAuthStatus';
import { GlobalCfg } from '../../conf';

export const LogOut = () => {
    const handleLogout = () => {
        analytics.event({ type: EventType.LogOutEvent });
        isLoggedInVar(false);
        Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
    };

    handleLogout();

    return <Redirect to="/" />;
};
