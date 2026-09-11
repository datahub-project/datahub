import {
    BROWSE_PATH_V2_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    PLATFORM_FILTER_NAME,
} from '@app/searchV2/utils/constants';

import { FacetFilterInput } from '@types';

export const BROWSE_NAVIGATION_FILTER_FIELDS = [PLATFORM_FILTER_NAME, BROWSE_PATH_V2_FILTER_NAME, ORIGIN_FILTER_NAME];

export function getEntitySubtypeFiltersForEntity(entityType: string, existingFilters: FacetFilterInput[]) {
    return existingFilters
        .find((f) => f.field === ENTITY_SUB_TYPE_FILTER_NAME)
        ?.values?.filter((value) => value.includes(entityType));
}

export function hasBrowseNavigationFilter(filters: FacetFilterInput[]): boolean {
    return filters.some((filter) => BROWSE_NAVIGATION_FILTER_FIELDS.includes(filter.field));
}

export function clearBrowseNavigationFilters(filters: FacetFilterInput[]): FacetFilterInput[] {
    return filters.filter((filter) => !BROWSE_NAVIGATION_FILTER_FIELDS.includes(filter.field));
}
