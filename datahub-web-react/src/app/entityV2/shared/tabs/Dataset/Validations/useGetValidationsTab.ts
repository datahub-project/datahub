/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

/**
 * The structure of our path will be
 *
 * /<entity-name>/<entity-urn>/Validation/<tab-name>
 */
const VALIDATION_TAB_NAME_REGEX_PATTERN = '^/[^/]+/[^/]+/[^/]+/([^/]+).*';

export type SelectedTab = {
    basePath: string;
    selectedTab: string | undefined;
};

/**
 * Returns information about the currently selected Validations Tab path.
 *
 * This is determined by parsing the current URL path and attempting to match against a set of
 * valid path names. If a matching tab cannot be found, then the selected tab will be returned as undefined.
 */
export const useGetValidationsTab = (pathname: string, tabNames: string[]): SelectedTab => {
    const trimmedPathName = pathname.endsWith('/') ? pathname.slice(0, pathname.length - 1) : pathname;
    const match = trimmedPathName.match(VALIDATION_TAB_NAME_REGEX_PATTERN);
    if (match && match[1]) {
        const selectedTabPath = match[1];
        const routedTab = tabNames.find((tab) => tab === selectedTabPath);
        return {
            basePath: trimmedPathName.substring(0, trimmedPathName.lastIndexOf('/')),
            selectedTab: routedTab,
        };
    }
    // No match found!
    return {
        basePath: trimmedPathName,
        selectedTab: undefined,
    };
};
