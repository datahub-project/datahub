
import queryString from 'query-string';
import { useLocation } from 'react-router';

import EntityRegistry from '@app/entity/EntityRegistry';
import { SEPARATE_SIBLINGS_URL_PARAM } from '@app/entity/shared/siblingUtils';
import { EntityTab, GenericEntityProperties } from '@app/entity/shared/types';

import { EntityType } from '@types';

/**
 * The structure of our path will be
 *
 * /<entity-name>/<entity-urn>/<tab-name>
 */
const ENTITY_TAB_NAME_REGEX_PATTERN = '^/[^/]+/[^/]+/([^/]+).*';

export function getEntityPath(
    entityType: EntityType,
    urn: string,
    entityRegistry: EntityRegistry,
    isLineageMode: boolean,
    isHideSiblingMode: boolean,
    tabName?: string,
    tabParams?: Record<string, any>,
) {
    const tabParamsString = tabParams ? `&${queryString.stringify(tabParams)}` : '';

    if (!tabName) {
        return `${entityRegistry.getEntityUrl(entityType, urn)}?is_lineage_mode=${isLineageMode}${tabParamsString}`;
    }
    return `${entityRegistry.getEntityUrl(entityType, urn)}/${tabName}?is_lineage_mode=${isLineageMode}${
        isHideSiblingMode ? `&${SEPARATE_SIBLINGS_URL_PARAM}=${isHideSiblingMode}` : ''
    }${tabParamsString}`;
}

export function useGlossaryActiveTabPath(): string {
    const { pathname, search } = useLocation();
    const trimmedPathName = pathname.endsWith('/') ? pathname.slice(0, pathname.length - 1) : pathname;

    // Match against the regex
    const match = trimmedPathName.match(ENTITY_TAB_NAME_REGEX_PATTERN);

    if (match && match[1]) {
        const selectedTabPath = match[1] + (search || ''); // Include all query parameters
        return selectedTabPath;
    }

    // No match found!
    return '';
}
