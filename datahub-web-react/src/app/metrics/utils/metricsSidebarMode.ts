/** Filters that start behind "+ Filter" until promoted. Primary row is Platform / Domain / Term. */
export type SecondaryBrowseFilter = 'tag' | 'owners';

export const SECONDARY_BROWSE_FILTERS: SecondaryBrowseFilter[] = ['tag', 'owners'];

export function isSecondaryBrowseFilter(value: string): value is SecondaryBrowseFilter {
    return (SECONDARY_BROWSE_FILTERS as string[]).includes(value);
}

export type MetricsSidebarSearchActiveInput = {
    /** Immediate search box text — drives browse vs search chrome (no debounce lag). */
    searchInput: string;
    platformUrns: string[];
    domainUrns: string[];
    tagUrns: string[];
    termUrns: string[];
    ownerUrns: string[];
};

/**
 * True when the metrics sidebar should show flat search results instead of the tree.
 * Any Platform/Domain/Tag/Term/Owner selection or non-empty query activates search.
 */
export function isMetricsSidebarSearchActive({
    searchInput,
    platformUrns,
    domainUrns,
    tagUrns,
    termUrns,
    ownerUrns,
}: MetricsSidebarSearchActiveInput): boolean {
    if (searchInput.trim().length > 0) return true;
    if (platformUrns.length > 0) return true;
    if (domainUrns.length > 0) return true;
    if (tagUrns.length > 0) return true;
    if (termUrns.length > 0) return true;
    if (ownerUrns.length > 0) return true;
    return false;
}

/**
 * Auto-promote secondary filters that already have values
 * so their controls stay visible without an extra "+ Filter" click.
 */
export function nextPromotedBrowseFilters(
    prev: ReadonlySet<SecondaryBrowseFilter>,
    {
        tagUrns,
        ownerUrns,
    }: {
        tagUrns: string[];
        ownerUrns: string[];
    },
): Set<SecondaryBrowseFilter> {
    const next = new Set(prev);
    if (tagUrns.length > 0) next.add('tag');
    if (ownerUrns.length > 0) next.add('owners');
    if (next.size === prev.size && [...next].every((key) => prev.has(key))) {
        return prev instanceof Set ? prev : new Set(prev);
    }
    return next;
}
