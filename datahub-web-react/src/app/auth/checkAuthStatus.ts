/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
