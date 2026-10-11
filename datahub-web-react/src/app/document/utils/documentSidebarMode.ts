import { DEFAULT_STATUS_FILTER, DocumentStatusFilter } from '@app/document/utils/documentTreeFilters';

/** Filters that start behind "+ Filter" until promoted. Primary row is Domain / Term / Type. */
export type SecondaryBrowseFilter = 'status' | 'author' | 'source' | 'tag';

export const SECONDARY_BROWSE_FILTERS: SecondaryBrowseFilter[] = ['tag', 'status', 'author', 'source'];

export function isSecondaryBrowseFilter(value: string): value is SecondaryBrowseFilter {
    return (SECONDARY_BROWSE_FILTERS as string[]).includes(value);
}

export type DocumentSidebarSearchActiveInput = {
    /** Immediate search box text — drives browse vs search chrome (no debounce lag). */
    searchInput: string;
    typeNames: string[];
    domainUrns: string[];
    tagUrns: string[];
    termUrns: string[];
    authorUrns: string[];
    platformUrns: string[];
    status: DocumentStatusFilter;
};

/**
 * True when the documents sidebar should show flat search results instead of the tree.
 * Any Type/Domain/Tag/Term/Author/Source/Status selection or non-empty query activates search.
 */
export function isDocumentSidebarSearchActive({
    searchInput,
    typeNames,
    domainUrns,
    tagUrns,
    termUrns,
    authorUrns,
    platformUrns,
    status,
}: DocumentSidebarSearchActiveInput): boolean {
    if (searchInput.trim().length > 0) return true;
    if (typeNames.length > 0) return true;
    if (domainUrns.length > 0) return true;
    if (tagUrns.length > 0) return true;
    if (termUrns.length > 0) return true;
    if (authorUrns.length > 0) return true;
    if (platformUrns.length > 0) return true;
    if (status !== DEFAULT_STATUS_FILTER) return true;
    return false;
}

/**
 * Auto-promote secondary filters that already have values (e.g. restored from context)
 * so their controls stay visible without an extra "+ Filter" click.
 */
export function nextPromotedBrowseFilters(
    prev: ReadonlySet<SecondaryBrowseFilter>,
    {
        status,
        authorUrns,
        platformUrns,
        tagUrns,
    }: {
        status: DocumentStatusFilter;
        authorUrns: string[];
        platformUrns: string[];
        tagUrns: string[];
    },
): Set<SecondaryBrowseFilter> {
    const next = new Set(prev);
    if (status !== DEFAULT_STATUS_FILTER) next.add('status');
    if (authorUrns.length > 0) next.add('author');
    if (platformUrns.length > 0) next.add('source');
    if (tagUrns.length > 0) next.add('tag');
    if (next.size === prev.size && [...next].every((key) => prev.has(key))) {
        return prev instanceof Set ? prev : new Set(prev);
    }
    return next;
}
