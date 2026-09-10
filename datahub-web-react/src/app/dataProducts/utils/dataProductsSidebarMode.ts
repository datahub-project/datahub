/** Filters that start behind "+ Filter" until promoted. Primary row is Domain / Owner / Tag. */
export type SecondaryBrowseFilter = 'term' | 'application';

export const SECONDARY_BROWSE_FILTERS: SecondaryBrowseFilter[] = ['term', 'application'];

export function isSecondaryBrowseFilter(value: string): value is SecondaryBrowseFilter {
    return (SECONDARY_BROWSE_FILTERS as string[]).includes(value);
}

export type DataProductsSidebarSearchActiveInput = {
    /** Immediate search box text — drives browse vs search chrome (no debounce lag). */
    searchInput: string;
    domainUrns: string[];
    tagUrns: string[];
    termUrns: string[];
    ownerUrns: string[];
    applicationUrns: string[];
};

export type DataProductsSidebarFilterSelection = Omit<DataProductsSidebarSearchActiveInput, 'searchInput'>;

export type DataProductsSidebarSearchModeState = {
    isSearchActive: boolean;
    shouldFetchSearch: boolean;
    searchQuery: string;
};

/** Derives immediate vs debounced search mode flags for sidebar chrome and fetch. */
export function buildDataProductsSearchModeState({
    searchInput,
    debouncedSearchInput,
    filters,
}: {
    searchInput: string;
    debouncedSearchInput: string;
    filters: DataProductsSidebarFilterSelection;
}): DataProductsSidebarSearchModeState {
    const isSearchActive = isDataProductsSidebarSearchActive({
        ...filters,
        searchInput,
    });
    const shouldFetchSearch = isDataProductsSidebarSearchActive({
        ...filters,
        searchInput: debouncedSearchInput,
    });

    return {
        isSearchActive,
        shouldFetchSearch,
        searchQuery: dataProductsSidebarSearchQuery(debouncedSearchInput),
    };
}

/** Normalizes sidebar search text for scrollAcrossEntities (`*` when empty). */
export function dataProductsSidebarSearchQuery(searchInput: string): string {
    const trimmed = searchInput.trim();
    return trimmed.length > 0 ? trimmed : '*';
}

/**
 * True when the data products sidebar should show flat search results instead of the tree.
 * Any Domain/Owner/Tag/Term/Application selection or non-empty query activates search.
 */
export function isDataProductsSidebarSearchActive({
    searchInput,
    domainUrns,
    tagUrns,
    termUrns,
    ownerUrns,
    applicationUrns,
}: DataProductsSidebarSearchActiveInput): boolean {
    if (searchInput.trim().length > 0) return true;
    if (domainUrns.length > 0) return true;
    if (ownerUrns.length > 0) return true;
    if (tagUrns.length > 0) return true;
    if (termUrns.length > 0) return true;
    if (applicationUrns.length > 0) return true;
    return false;
}

/**
 * Auto-promote secondary filters that already have values
 * so their controls stay visible without an extra "+ Filter" click.
 */
export function nextPromotedBrowseFilters(
    prev: ReadonlySet<SecondaryBrowseFilter>,
    {
        termUrns,
        applicationUrns,
    }: {
        termUrns: string[];
        applicationUrns: string[];
    },
): Set<SecondaryBrowseFilter> {
    const next = new Set(prev);
    if (termUrns.length > 0) next.add('term');
    if (applicationUrns.length > 0) next.add('application');
    if (next.size === prev.size && [...next].every((key) => prev.has(key))) {
        return prev instanceof Set ? prev : new Set(prev);
    }
    return next;
}
