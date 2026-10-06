import { getEntityTypesPropertyFilter, getNotHiddenPropertyFilter } from '@src/app/govern/structuredProperties/utils';
import {
    SHOW_IN_ASSET_SUMMARY_PROPERTY_FILTER_NAME,
    SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME,
} from '@src/app/searchV2/utils/constants';
import { EntityRegistry } from '@src/entityRegistryContext';
import { EntityType } from '@src/types.generated';

export function getSidebarStructuredPropertiesOrFilters(
    isSchemaSidebar: boolean,
    entityRegistry: EntityRegistry,
    entityType: EntityType,
) {
    const propertyFilters = {
        and: [
            getEntityTypesPropertyFilter(entityRegistry, isSchemaSidebar, entityType),
            getNotHiddenPropertyFilter(),
            { field: SHOW_IN_ASSET_SUMMARY_PROPERTY_FILTER_NAME, values: ['true'] },
        ],
    };
    const schemaFieldPropertyFilters = {
        and: [
            getEntityTypesPropertyFilter(entityRegistry, isSchemaSidebar, entityType),
            getNotHiddenPropertyFilter(),
            { field: SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME, values: ['true'] },
        ],
    };
    return isSchemaSidebar ? [propertyFilters, schemaFieldPropertyFilters] : [propertyFilters];
}
