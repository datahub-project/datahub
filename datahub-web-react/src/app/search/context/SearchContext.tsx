/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useContext } from 'react';

export type SearchContextType = {
    query: string | undefined;
    selectedSortOption: string | undefined;
    setSelectedSortOption: (sortOption: string) => void;
    isFullViewCard: boolean;
    setIsFullViewCard: (isFullViewCard: boolean) => void;
};

const DEFAULT_CONTEXT = {
    query: undefined,
    selectedSortOption: undefined,
    isFullViewCard: false,
    setSelectedSortOption: (_: string) => null,
    setIsFullViewCard: (_: boolean) => null,
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
