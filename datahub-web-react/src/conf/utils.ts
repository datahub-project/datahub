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

import { HelpLinkRoutes } from '@conf/Global';

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

/**
 * Generates link to release notes for provided version
 *
 * e.g.
 * v0.3.7rc1 -> https://datahubproject.io/docs/managed-datahub/release-notes/v_0_3_7/
 */
export const generateReleaseNotesLink = (version: string | null | undefined): string | undefined => {
    if (!version) return undefined;

    const pattern = /^v(\d+\.\d+\.\d+).*$/;
    const linkVersion = version.match(pattern)?.[1]?.replaceAll('.', '_');
    if (!linkVersion) return undefined;

    return HelpLinkRoutes.RELEASE_NOTES_TEMPLATE.replace('{version}', linkVersion);
};
