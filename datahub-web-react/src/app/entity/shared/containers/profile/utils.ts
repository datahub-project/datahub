import { useEffect } from 'react';
import { useLocation } from 'react-router';
import queryString from 'query-string';
import { isEqual } from 'lodash';
import { AppConfig, EntityType } from '../../../../../types.generated';
import useIsLineageMode from '../../../../lineage/utils/useIsLineageMode';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import EntityRegistry from '../../../EntityRegistry';
import { EntityTab, GenericEntityProperties } from '../../types';
import { useIsSeparateSiblingsMode, SEPARATE_SIBLINGS_URL_PARAM } from '../../siblingUtils';
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
} from '../../../../onboarding/config/EntityProfileOnboardingConfig';
import { useGlossaryEntityData } from '../../GlossaryEntityContext';
import usePrevious from '../../../../shared/usePrevious';
import { GLOSSARY_ENTITY_TYPES } from '../../constants';

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

    if (anyEntityData?.siblings?.siblings?.filter((sibling) => sibling.exists).length > 0 && !isHideSiblingMode) {
        const genericSiblingProperties: GenericEntityProperties[] = anyEntityData?.siblings?.siblings?.map((sibling) =>
            getDataForEntityType({ data: sibling, getOverrideProperties: () => ({}) }),
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
                ENTITY_PROFILE_DOCUMENTATION_ID,
                ENTITY_PROFILE_PROPERTIES_ID,
                ENTITY_PROFILE_LINEAGE_ID,
                ENTITY_PROFILE_TAGS_ID,
                ENTITY_PROFILE_GLOSSARY_TERMS_ID,
                ENTITY_PROFILE_OWNERS_ID,
                ENTITY_PROFILE_DOMAINS_ID,
            ];
        case EntityType.Container:
            return [
                ENTITY_PROFILE_ENTITIES_ID,
                ENTITY_PROFILE_DOCUMENTATION_ID,
                ENTITY_PROFILE_PROPERTIES_ID,
                ENTITY_PROFILE_OWNERS_ID,
                ENTITY_PROFILE_TAGS_ID,
                ENTITY_PROFILE_GLOSSARY_TERMS_ID,
                ENTITY_PROFILE_DOMAINS_ID,
            ];
        case EntityType.Dataset:
            return [
                ENTITY_PROFILE_SCHEMA_ID,
                ENTITY_PROFILE_DOCUMENTATION_ID,
                ENTITY_PROFILE_LINEAGE_ID,
                ENTITY_PROFILE_PROPERTIES_ID,
                ENTITY_PROFILE_OWNERS_ID,
                ENTITY_PROFILE_TAGS_ID,
                ENTITY_PROFILE_GLOSSARY_TERMS_ID,
                ENTITY_PROFILE_DOMAINS_ID,
            ];
            break;
        default:
            return [];
    }
}

function sortTabsWithDefaultTabId(tabs: EntityTab[], defaultTabId: string) {
    return tabs.sort((tabA, tabB) => {
        if (tabA.id === defaultTabId) return -1;
        if (tabB.id === defaultTabId) return 1;
        return 0;
    });
}

export function sortEntityProfileTabs(appConfig: AppConfig, entityType: EntityType, tabs: EntityTab[]) {
    const sortedTabs = [...tabs];

    if (entityType === EntityType.Domain && appConfig.visualConfig.entityProfiles?.domain?.defaultTab) {
        const defaultTabId = appConfig.visualConfig.entityProfiles?.domain?.defaultTab;
        sortTabsWithDefaultTabId(sortedTabs, defaultTabId);
    }

    return sortedTabs;
}

export function getNestedValue(obj: any, path: string) {
    return path.split('.').reduce((o, p) => (o || {})[p], obj);
}
