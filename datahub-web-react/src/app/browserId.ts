/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import Cookies from 'js-cookie';
import { v4 as uuidv4 } from 'uuid';

import { BROWSER_ID_COOKIE } from '@conf/Global';

function generateBrowserId(): string {
    return uuidv4();
}

export function getBrowserId() {
    let browserId = Cookies.get(BROWSER_ID_COOKIE);
    if (!browserId) {
        browserId = generateBrowserId();
        Cookies.set(BROWSER_ID_COOKIE, browserId, { expires: 365 });
    }
    return browserId;
}
