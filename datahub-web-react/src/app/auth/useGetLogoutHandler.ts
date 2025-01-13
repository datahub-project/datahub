import { useCallback } from 'react';
import Cookies from 'js-cookie';
import { GlobalCfg } from '@src/conf';
import { useHistory } from 'react-router-dom';
import analytics, { EventType } from '../analytics';
import { useUserContext } from '../context/useUserContext';
import { isLoggedInVar } from './checkAuthStatus';

export default function useGetLogoutHandler() {
    const me = useUserContext();
    const history = useHistory();

    return useCallback(() => {
        analytics.event({ type: EventType.LogOutEvent });
        isLoggedInVar(false);
        Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
        me.updateLocalState({ selectedViewUrn: undefined });
        // must go to /logOut for datahub-frontend to fully log you out
        history.replace('/logOut');
    }, [me, history]);
}
