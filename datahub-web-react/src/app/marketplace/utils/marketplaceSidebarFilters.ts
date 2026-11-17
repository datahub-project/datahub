import {
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';

import { FacetFilterInput, FilterOperator } from '@types';

export const APPLICATIONS_FILTER_NAME = 'applications';

export type MarketplaceSidebarFilterInput = {
    domainUrns?: string[];
    tagUrns?: string[];
    termUrns?: string[];
    ownerUrns?: string[];
    applicationUrns?: string[];
};

/**
 * Builds FacetFilterInput[] for the marketplace sidebar (scroll + dependent facets).
 */
export function buildMarketplaceSidebarFilters({
    domainUrns = [],
    tagUrns = [],
    termUrns = [],
    ownerUrns = [],
    applicationUrns = [],
}: MarketplaceSidebarFilterInput): FacetFilterInput[] {
    const next: FacetFilterInput[] = [];
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
    if (applicationUrns.length > 0) {
        next.push({ field: APPLICATIONS_FILTER_NAME, condition: FilterOperator.Equal, values: applicationUrns });
    }
    return next;
}
