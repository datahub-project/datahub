import { isEqual } from 'lodash';
import queryString from 'query-string';
import { useEffect } from 'react';
import { useLocation } from 'react-router';

import EntityRegistry from '@app/entity/EntityRegistry';
import { GLOSSARY_ENTITY_TYPES } from '@app/entity/shared/constants';
import { SEPARATE_SIBLINGS_URL_PARAM, useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import { EntityTab, GenericEntityProperties } from '@app/entity/shared/types';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import {
    ENTITY_PROFILE_DOCUMENTATION_ID,
    ENTITY_PROFILE_DOMAINS_ID,
    ENTITY_PROFILE_ENTITIES_ID,
    ENTITY_PROFILE_GLOSSARY_TERMS_ID,
    ENTITY_PROFILE_LINEAGE_ID,
    ENTITY_PROFILE_OWNERS_ID,
    ENTITY_PROFILE_PROPERTIES_ID,
    ENTITY_PROFILE_SCHEMA_ID,
    ENTITY_PROFILE_TAGS_ID,
} from '@app/onboarding/config/EntityProfileOnboardingConfig';
import usePrevious from '@app/shared/usePrevious';

import { AppConfig, EntityType } from '@types';

/**
 * The structure of our path will be
 *
 * /<entity-name>/<entity-urn>/<tab-name>
 */
const ENTITY_TAB_NAME_REGEX_PATTERN = '^/[^/]+/[^/]+/([^/]+).*';

export function getDataForEntityType<T>({
    data: entityData,
    getOverrideProperties,
    isHideSiblingMode,
}: {
    data: T;
    entityType?: EntityType;
    getOverrideProperties: (T) => GenericEntityProperties;
    isHideSiblingMode?: boolean;
}): GenericEntityProperties | null {
    if (!entityData) {
        return null;
    }
    const anyEntityData = entityData as any;
    let modifiedEntityData = entityData;
    // Bring 'customProperties' field to the root level.
    const customProperties = anyEntityData.properties?.customProperties || anyEntityData.info?.customProperties;
    if (customProperties) {
        modifiedEntityData = {
            ...entityData,
            customProperties,
        };
    }
    if (anyEntityData.tags) {
        modifiedEntityData = {
            ...modifiedEntityData,
            globalTags: anyEntityData.tags,
        };
    }

    if (
        anyEntityData?.siblingsSearch?.searchResults?.filter((sibling) => sibling.entity.exists).length > 0 &&
        !isHideSiblingMode
    ) {
        const genericSiblingProperties: GenericEntityProperties[] = anyEntityData?.siblingsSearch?.searchResults?.map(
            (sibling) => getDataForEntityType({ data: sibling.entity, getOverrideProperties: () => ({}) }),
        );

        const allPlatforms = anyEntityData.siblings.isPrimary
            ? [anyEntityData.platform, genericSiblingProperties?.[0]?.platform]
            : [genericSiblingProperties?.[0]?.platform, anyEntityData.platform];

        modifiedEntityData = {
            ...modifiedEntityData,
            siblingPlatforms: allPlatforms,
        };
    }

    return {
        ...modifiedEntityData,
        ...getOverrideProperties(entityData),
    };
}

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

export function useIsOnTab(tabName: string): boolean {
    const { pathname } = useLocation();
    const trimmedPathName = pathname.endsWith('/') ? pathname.slice(0, pathname.length - 1) : pathname;
    // Match against the regex
    const match = trimmedPathName.match(ENTITY_TAB_NAME_REGEX_PATTERN);
    if (match && match[1]) {
        const selectedTabPath = match[1];
        return selectedTabPath === tabName;
    }
    // No match found!
    return false;
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

export function formatDateString(time: number) {
    const date = new Date(time);
    return date.toLocaleDateString('en-US');
}

function sortTabsWithDefaultTabId(tabs: EntityTab[], defaultTabId: string) {
    return tabs.sort((tabA, tabB) => {
        if (tabA.id === defaultTabId) return -1;
        if (tabB.id === defaultTabId) return 1;
        return 0;
    });
}
