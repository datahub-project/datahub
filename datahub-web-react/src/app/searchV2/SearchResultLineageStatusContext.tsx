import React, { ReactNode, createContext, useContext, useMemo } from 'react';

type SearchResultLineageStatus = {
    /** True when the deferred page-level lineage count query failed. */
    failed: boolean;
};

const defaultStatus: SearchResultLineageStatus = {
    failed: false,
};

const SearchResultLineageStatusContext = createContext<SearchResultLineageStatus>(defaultStatus);

type ProviderProps = {
    failed: boolean;
    children: ReactNode;
};

/**
 * Lets search cards clear the reserved lineage badge slot when the deferred count batch fails.
 * Outside search (or when the provider is absent) `failed` stays false.
 */
export function SearchResultLineageStatusProvider({ failed, children }: ProviderProps) {
    const value = useMemo(() => ({ failed }), [failed]);
    return (
        <SearchResultLineageStatusContext.Provider value={value}>{children}</SearchResultLineageStatusContext.Provider>
    );
}

export function useSearchResultLineageStatus(): SearchResultLineageStatus {
    return useContext(SearchResultLineageStatusContext);
}
