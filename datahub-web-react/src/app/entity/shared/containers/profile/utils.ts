import { useLocation } from 'react-router';
import queryString from 'query-string';
import { EntityType } from '../../../../../types.generated';
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

    if (anyEntityData?.siblings?.siblings?.length > 0 && !isHideSiblingMode) {
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
    const splitPathName = trimmedPathName.split('/');
    const lastTokenInPath = splitPathName[splitPathName.length - 1];
    const routedTab = tabs.find((tab) => tab.name === lastTokenInPath);
    return routedTab;
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
                ENTITY_PROFILE_PROPERTIES_ID,
                ENTITY_PROFILE_LINEAGE_ID,
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
