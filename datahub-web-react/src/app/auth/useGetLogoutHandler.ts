import { useCallback } from 'react';
import Cookies from 'js-cookie';
import { GlobalCfg } from '@src/conf';
import analytics, { EventType } from '../analytics';
import { useUserContext } from '../context/useUserContext';
import { isLoggedInVar } from './checkAuthStatus';

export default function useGetLogoutHandler() {
    const me = useUserContext();

    return useCallback(() => {
        analytics.event({ type: EventType.LogOutEvent });
        isLoggedInVar(false);
        Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
        me.updateLocalState({ selectedViewUrn: undefined });
    }, [me]);
}
