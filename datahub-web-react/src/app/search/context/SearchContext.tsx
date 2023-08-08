import React from 'react';

export type SearchContextType = {
    selectedSortOption: string;
    setSelectedSortOption: (sortOption: string) => void;
};

export const DEFAULT_CONTEXT = {
    selectedSortOption: '',
    setSelectedSortOption: (_: string) => null,
};

export const SearchContext = React.createContext<SearchContextType>(DEFAULT_CONTEXT);
export default SearchContext;
