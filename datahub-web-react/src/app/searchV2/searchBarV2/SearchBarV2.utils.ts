/**
 * Whether SearchBarV2 should run typeahead GraphQL.
 *
 * Skips mount-time URL query seeding and inactive selectedView resolution; only
 * fetches when the user is using the bar and the query is user-driven.
 */
export function shouldFetchSearchBarTypeahead({
    isSearchBarActive,
    hasUserTyped,
    searchQuery,
    pageOpenQuery,
}: {
    isSearchBarActive: boolean;
    hasUserTyped: boolean;
    searchQuery: string;
    pageOpenQuery: string;
}): boolean {
    return isSearchBarActive && (hasUserTyped || searchQuery !== pageOpenQuery);
}
