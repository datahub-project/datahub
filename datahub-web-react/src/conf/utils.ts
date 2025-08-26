import { HelpLinkRoutes } from '@conf/Global';

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
