import Cookies from 'js-cookie';
import { useCallback } from 'react';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import { useUserContext } from '@app/context/useUserContext';
import { GlobalCfg } from '@src/conf';

export default function useGetLogoutHandler() {
    const me = useUserContext();

    return useCallback(() => {
        analytics.event({ type: EventType.LogOutEvent });
        isLoggedInVar(false);
        Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
        me.updateLocalState({ selectedViewUrn: undefined });
    }, [me]);
}
