import {
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';

import { FacetFilterInput, FilterOperator } from '@types';

export type MetricsSidebarFilterInput = {
    platformUrns?: string[];
    domainUrns?: string[];
    tagUrns?: string[];
    termUrns?: string[];
    ownerUrns?: string[];
};

/**
 * Builds FacetFilterInput[] for the metrics sidebar (search + dependent facets).
 * Empty arrays omit that field. Callers AND via generateOrFilters.
 */
export function buildMetricsSidebarFilters({
    platformUrns = [],
    domainUrns = [],
    tagUrns = [],
    termUrns = [],
    ownerUrns = [],
}: MetricsSidebarFilterInput): FacetFilterInput[] {
    const next: FacetFilterInput[] = [];
    if (platformUrns.length > 0) {
        next.push({ field: PLATFORM_FILTER_NAME, condition: FilterOperator.Equal, values: platformUrns });
    }
    if (domainUrns.length > 0) {
        next.push({ field: DOMAINS_FILTER_NAME, condition: FilterOperator.Equal, values: domainUrns });
    }
    if (tagUrns.length > 0) {
        next.push({ field: TAGS_FILTER_NAME, condition: FilterOperator.Equal, values: tagUrns });
    }
    if (termUrns.length > 0) {
        next.push({ field: GLOSSARY_TERMS_FILTER_NAME, condition: FilterOperator.Equal, values: termUrns });
    }
    if (ownerUrns.length > 0) {
        next.push({ field: OWNERS_FILTER_NAME, condition: FilterOperator.Equal, values: ownerUrns });
    }
    return next;
}
