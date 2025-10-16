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
