/**
 *
 * as per the new route object
 * We are redirecting older routes to new one
 * e.g. 
 * {
         '/Validation/Assertions': '/Quality/List',
    }
 *  */

export const getRedirectUrl = (newRoutes: { [key: string]: string }) => {
    let newPathname = `${window.location.pathname}${window.location.search}`;
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

    return `${newPathname}${window.location.search}`;
};
