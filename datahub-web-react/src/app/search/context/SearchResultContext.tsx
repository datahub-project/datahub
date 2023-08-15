import React, { ReactNode, createContext, useContext, useMemo } from 'react';
import { SearchResult } from '../../../types.generated';
import { MATCHED_FIELD_MAPPING, NormalizedMatchedFieldName } from '../highlight/utils';

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

export const useEntityType = () => {
    return useSearchResultContext()?.searchResult.entity.type;
};

const useMatchedFields = () => {
    return useSearchResultContext()?.searchResult.matchedFields;
};

export const useMatchedField = (normalizedFieldName?: NormalizedMatchedFieldName) => {
    const entityType = useEntityType();
    const fieldName =
        normalizedFieldName && entityType && entityType in MATCHED_FIELD_MAPPING
            ? MATCHED_FIELD_MAPPING[entityType][normalizedFieldName]
            : undefined;
    return useMatchedFields()?.find((field) => field.name === fieldName);
};
