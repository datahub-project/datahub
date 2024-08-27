import React, { useContext } from 'react';

export type SearchContextType = {
    query: string | undefined;
    selectedSortOption: string | undefined;
    setSelectedSortOption: (sortOption: string) => void;
};

export const DEFAULT_CONTEXT = {
    query: undefined,
    selectedSortOption: undefined,
    setSelectedSortOption: (_: string) => null,
};

export const SearchContext = React.createContext<SearchContextType>(DEFAULT_CONTEXT);

export function useSearchContext() {
    const context = useContext(SearchContext);
    if (context === null) throw new Error(`${useSearchContext.name} must be used under a SearchContextProvider`);
    return context;
}

export function useSelectedSortOption() {
    return useSearchContext().selectedSortOption;
}

export function useSearchQuery() {
    return useSearchContext().query;
}
