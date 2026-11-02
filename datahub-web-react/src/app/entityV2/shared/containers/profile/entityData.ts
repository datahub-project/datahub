import queryString from 'query-string';
import { useLocation } from 'react-router';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { getSchemaFieldParentLink } from '@app/entityV2/schemaField/utils';
import { SEPARATE_SIBLINGS_URL_PARAM, useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { EntityRegistry } from '@src/entityRegistryContext';

import { EntityType, FeatureFlagsConfig } from '@types';

/**
 * Kept separate from `utils.tsx` so search previews and the entity registry can read
 * display data without importing profile sidebar UI.
 *
 * Path shape: /<entity-name>/<entity-urn>/<tab-name>
 */
export const ENTITY_TAB_NAME_REGEX_PATTERN = '^/[^/]+/[^/]+/([^/]+).*';

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
    if (entityType === EntityType.SchemaField) {
        return getSchemaFieldParentLink(urn);
    }

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

    const match = trimmedPathName.match(ENTITY_TAB_NAME_REGEX_PATTERN);

    if (match && match[1]) {
        const selectedTabPath = match[1] + (search || ''); // Include all query parameters
        return selectedTabPath;
    }

    return '';
}

export function useEntityQueryParams() {
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const response = {};
    if (isHideSiblingMode) {
        response[SEPARATE_SIBLINGS_URL_PARAM] = true;
    }

    return response;
}
