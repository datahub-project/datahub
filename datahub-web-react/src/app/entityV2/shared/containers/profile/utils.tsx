import { ReadOutlined } from '@ant-design/icons';
import { isEqual } from 'lodash';
import queryString from 'query-string';
import React, { useEffect } from 'react';
import { useLocation } from 'react-router';
import { EntityRegistry } from '../../../../../entityRegistryContext';
import { EntityType, FeatureFlagsConfig } from '../../../../../types.generated';
import useIsLineageMode from '../../../../lineage/utils/useIsLineageMode';
import {
    ENTITY_PROFILE_DOMAINS_ID,
    ENTITY_PROFILE_GLOSSARY_TERMS_ID,
    ENTITY_PROFILE_LINEAGE_ID,
    ENTITY_PROFILE_OWNERS_ID,
    ENTITY_PROFILE_PROPERTIES_ID,
    ENTITY_PROFILE_TAGS_ID,
    ENTITY_PROFILE_V2_SIDEBAR_ID,
} from '../../../../onboarding/config/EntityProfileOnboardingConfig';
import {
    ENTITY_PROFILE_V2_COLUMNS_ID,
    ENTITY_PROFILE_V2_CONTENTS_ID,
    ENTITY_PROFILE_V2_DOCUMENTATION_ID,
    ENTITY_PROFILE_V2_INCIDENTS_ID,
    ENTITY_SIDEBAR_V2_PROPERTIES_ID,
    ENTITY_PROFILE_V2_QUERIES_ID,
    ENTITY_PROFILE_V2_VALIDATION_ID,
    ENTITY_SIDEBAR_V2_LINEAGE_TAB_ID,
    ENTITY_SIDEBAR_V2_COLUMNS_TAB_ID,
    ENTITY_SIDEBAR_V2_ABOUT_TAB_ID,
} from '../../../../onboarding/configV2/EntityProfileOnboardingConfig';
import usePrevious from '../../../../shared/usePrevious';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { GLOSSARY_ENTITY_TYPES } from '../../constants';
import { useGlossaryEntityData } from '../../GlossaryEntityContext';
import { SEPARATE_SIBLINGS_URL_PARAM, useIsSeparateSiblingsMode } from '../../useIsSeparateSiblingsMode';
import { EntitySidebarSection, EntitySidebarTab, EntityTab } from '../../types';
import { GenericEntityProperties } from '../../../../entity/shared/types';
import EntitySidebarSectionsTab from './sidebar/EntitySidebarSectionsTab';
import SidebarPopularityHeaderSection from './sidebar/shared/SidebarPopularityHeaderSection';
import { PopularityTier, getBarsStatusFromPopularityTier } from './sidebar/shared/utils';

/**
 * The structure of our path will be
 *
 * /<entity-name>/<entity-urn>/<tab-name>
 */
const ENTITY_TAB_NAME_REGEX_PATTERN = '^/[^/]+/[^/]+/([^/]+).*';

export type SidebarStatsColumn = {
    title: React.ReactNode;
    content: React.ReactNode;
};

export function getDataForEntityType<T>({
    data: entityData,
    getOverrideProperties,
    isHideSiblingMode,
    flags,
}: {
    data: T;
    entityType?: EntityType;
    getOverrideProperties?: (T, flags?: FeatureFlagsConfig) => GenericEntityProperties;
    isHideSiblingMode?: boolean;
    flags?: FeatureFlagsConfig;
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

        const allPlatforms = anyEntityData.siblings?.isPrimary
            ? [anyEntityData.platform, genericSiblingProperties?.[0]?.platform]
            : [genericSiblingProperties?.[0]?.platform, anyEntityData.platform];

        modifiedEntityData = {
            ...modifiedEntityData,
            siblingPlatforms: allPlatforms,
        };
    }

    return {
        ...modifiedEntityData,
        ...getOverrideProperties?.(entityData, flags),
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

export function useEntityPath(entityType: EntityType, urn: string, tabName?: string, tabParams?: Record<string, any>) {
    const isLineageMode = useIsLineageMode();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const entityRegistry = useEntityRegistry();
    return getEntityPath(entityType, urn, entityRegistry, isLineageMode, isHideSiblingMode, tabName, tabParams);
}

export function useRoutedTab(tabs: EntityTab[]): EntityTab | undefined {
    const { pathname } = useLocation();
    const trimmedPathName = pathname.endsWith('/') ? pathname.slice(0, pathname.length - 1) : pathname;
    // Match against the regex
    const match = trimmedPathName.match(ENTITY_TAB_NAME_REGEX_PATTERN);
    if (match && match[1]) {
        const selectedTabPath = match[1];
        const routedTab = tabs.find((tab) => tab.name === selectedTabPath);
        return routedTab;
    }
    // No match found!
    return undefined;
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

export function useEntityQueryParams() {
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const response = {};
    if (isHideSiblingMode) {
        response[SEPARATE_SIBLINGS_URL_PARAM] = true;
    }

    return response;
}

export function useUpdateGlossaryEntityDataOnChange(
    entityData: GenericEntityProperties | null,
    entityType: EntityType,
) {
    const { setEntityData } = useGlossaryEntityData();
    const previousEntityData = usePrevious(entityData);

    useEffect(() => {
        // first check this is a glossary entity to prevent unnecessary comparisons in non-glossary context
        if (GLOSSARY_ENTITY_TYPES.includes(entityType) && !isEqual(entityData, previousEntityData)) {
            setEntityData(entityData);
        }
    });
}

export function getOnboardingStepIdsForEntityType(entityType: EntityType): string[] {
    switch (entityType) {
        case EntityType.Chart:
            return [
                ENTITY_PROFILE_V2_DOCUMENTATION_ID,
                ENTITY_PROFILE_PROPERTIES_ID,
                ENTITY_PROFILE_LINEAGE_ID,
                ENTITY_PROFILE_TAGS_ID,
                ENTITY_PROFILE_GLOSSARY_TERMS_ID,
                ENTITY_PROFILE_OWNERS_ID,
                ENTITY_PROFILE_DOMAINS_ID,
            ];
        case EntityType.Container:
            return [
                ENTITY_PROFILE_V2_CONTENTS_ID,
                ENTITY_PROFILE_V2_DOCUMENTATION_ID,
                ENTITY_PROFILE_PROPERTIES_ID,
                ENTITY_PROFILE_OWNERS_ID,
                ENTITY_PROFILE_TAGS_ID,
                ENTITY_PROFILE_GLOSSARY_TERMS_ID,
                ENTITY_PROFILE_DOMAINS_ID,
            ];
        case EntityType.Dataset:
            return [
                ENTITY_PROFILE_V2_SIDEBAR_ID,
                ENTITY_SIDEBAR_V2_ABOUT_TAB_ID,
                ENTITY_SIDEBAR_V2_LINEAGE_TAB_ID,
                ENTITY_SIDEBAR_V2_COLUMNS_TAB_ID,
                ENTITY_SIDEBAR_V2_PROPERTIES_ID,
                ENTITY_PROFILE_DOMAINS_ID,
                ENTITY_PROFILE_OWNERS_ID,
                ENTITY_PROFILE_TAGS_ID,
                ENTITY_PROFILE_GLOSSARY_TERMS_ID,
                ENTITY_PROFILE_V2_COLUMNS_ID,
                ENTITY_PROFILE_V2_DOCUMENTATION_ID,
                ENTITY_PROFILE_LINEAGE_ID,
                ENTITY_PROFILE_V2_QUERIES_ID,
                ENTITY_PROFILE_V2_VALIDATION_ID,
                ENTITY_PROFILE_V2_INCIDENTS_ID,
            ];
        default:
            return [];
    }
}

export const defaultTabDisplayConfig = {
    visible: (_, _1) => true,
    enabled: (_, _1) => true,
};

export const getFinalSidebarTabs = (tabs: EntitySidebarTab[], sidebarSections: EntitySidebarSection[]) => {
    const sidebarTabsWithDefaults = tabs.map((tab) => ({
        ...tab,
        display: { ...defaultTabDisplayConfig, ...tab.display },
    }));

    let finalTabs = sidebarTabsWithDefaults;

    // Add a default "About" tab if only the legacy sections were provided.
    if ((sidebarSections || [])?.length > 0) {
        finalTabs = [
            {
                name: 'About',
                icon: ReadOutlined,
                component: EntitySidebarSectionsTab,
                properties: {
                    sections: sidebarSections || [],
                },
                display: {
                    ...defaultTabDisplayConfig,
                },
            },
            ...sidebarTabsWithDefaults,
        ];
    }

    return finalTabs;
};

export function getPopularityColumn(tier: PopularityTier): SidebarStatsColumn | null {
    if (tier === undefined) return null;

    const status = getBarsStatusFromPopularityTier(tier);
    if (status) {
        return {
            title: 'Popularity',
            content: <SidebarPopularityHeaderSection />,
        };
    }
    return null;
}
