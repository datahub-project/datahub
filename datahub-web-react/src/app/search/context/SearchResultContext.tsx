import React, { ReactNode, createContext, useContext, useMemo } from 'react';
import { SearchResult } from '../../../types.generated';

export type HighlightField = 'urn' | 'id' | 'name' | 'description';

type SearchResultContextValue = {
    searchResult: SearchResult;
} | null;

const SearchResultContext = createContext<SearchResultContextValue>(null);

type Props = {
    children: ReactNode;
    searchResult: SearchResult;
};

export const SearchResultProvider = ({ children, searchResult }: Props) => {
    const value = useMemo(
        () => ({
            searchResult,
        }),
        [searchResult],
    );
    return <SearchResultContext.Provider value={value}>{children}</SearchResultContext.Provider>;
};

const useSearchResultContext = () => {
    return useContext(SearchResultContext);
};

// const useEntityType = () => {
//     return useSearchResultContext()?.searchResult.entity.type;
// };

const useMatchedFields = () => {
    return useSearchResultContext()?.searchResult.matchedFields;
};

export const useMatchedField = (fieldName?: HighlightField) => {
    return useMatchedFields()?.find((field) => field.name === fieldName);
};
