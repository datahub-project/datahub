/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
/**
 *
 * as per the new route object
 * We are redirecting older routes to new one
 * e.g. 
 * {
         '/Validation/Assertions': '/Quality/List',
    }
 *  */
import { Location } from 'history';

export const getRedirectUrl = (newRoutes: { [key: string]: string }, location: Location) => {
    let newPathname = `${location.pathname}${location.search}`;
    if (!newRoutes) {
        return newPathname;
    }

    // eslint-disable-next-line no-restricted-syntax
    for (const path of Object.keys(newRoutes)) {
        if (newPathname.indexOf(path) !== -1) {
            newPathname = newPathname.replace(path, newRoutes[path]);
            break;
        }
    }

    return `${newPathname}${location.search}`;
};
