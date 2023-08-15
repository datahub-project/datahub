import React, { ReactNode, createContext, useContext, useMemo } from 'react';
import { SearchResult } from '../../../types.generated';
import { getMatchedFieldNames } from '../highlight/utils';
import { NormalizedMatchedFieldName } from '../highlight/constants';

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
    const matchedFields = useMatchedFields();
    const matchedFieldNames = getMatchedFieldNames(entityType, normalizedFieldName);
    return matchedFields?.find((field) => matchedFieldNames.includes(field.name));
};
