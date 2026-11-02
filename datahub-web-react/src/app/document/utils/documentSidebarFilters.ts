import { DocumentStatusFilter } from '@app/document/utils/documentTreeFilters';
import {
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
} from '@app/searchV2/utils/constants';

import { FacetFilterInput, FilterOperator } from '@types';

/** Indexed DocumentInfo.created.actor — Author filter field. */
export const DOCUMENT_CREATOR_FILTER_NAME = 'creator';

/** Indexed DocumentInfo.status.state — Status filter field. */
export const DOCUMENT_STATE_FILTER_NAME = 'state';

export type DocumentSidebarFilterInput = {
    typeNames?: string[];
    domainUrns?: string[];
    tagUrns?: string[];
    termUrns?: string[];
    authorUrns?: string[];
    platformUrns?: string[];
    status?: DocumentStatusFilter;
};

/**
 * Builds FacetFilterInput[] for the documents sidebar (search + dependent facets).
 * Empty arrays / status `all` omit that field. Callers AND via generateOrFilters.
 */
export function buildDocumentSidebarFilters({
    typeNames = [],
    domainUrns = [],
    tagUrns = [],
    termUrns = [],
    authorUrns = [],
    platformUrns = [],
    status = 'all',
}: DocumentSidebarFilterInput): FacetFilterInput[] {
    const next: FacetFilterInput[] = [];
    if (typeNames.length > 0) {
        next.push({ field: TYPE_NAMES_FILTER_NAME, condition: FilterOperator.Equal, values: typeNames });
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
    if (authorUrns.length > 0) {
        next.push({
            field: DOCUMENT_CREATOR_FILTER_NAME,
            condition: FilterOperator.Equal,
            values: authorUrns,
        });
    }
    if (platformUrns.length > 0) {
        next.push({ field: PLATFORM_FILTER_NAME, condition: FilterOperator.Equal, values: platformUrns });
    }
    if (status === 'published') {
        next.push({
            field: DOCUMENT_STATE_FILTER_NAME,
            condition: FilterOperator.Equal,
            values: ['PUBLISHED'],
        });
    } else if (status === 'unpublished') {
        next.push({
            field: DOCUMENT_STATE_FILTER_NAME,
            condition: FilterOperator.Equal,
            values: ['UNPUBLISHED'],
        });
    }
    return next;
}
