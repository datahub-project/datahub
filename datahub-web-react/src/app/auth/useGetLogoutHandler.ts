/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
