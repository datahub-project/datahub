import React, { ReactNode, createContext, useContext, useMemo } from 'react';
import { SearchResult } from '../../../types.generated';
import { getMatchedFieldByUrn, getMatchedFieldNames, getMatchedFieldsByNames } from './utils';
import { NormalizedMatchedFieldName } from './constants';

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

export const useIsSearchResult = () => {
    return !!useSearchResultContext();
};

const useMatchedFields = () => {
    return useSearchResultContext()?.searchResult.matchedFields;
};

export const useMatchedFieldsByNormalizedFieldName = (normalizedFieldName?: NormalizedMatchedFieldName) => {
    const matchedFields = useMatchedFields();
    const matchedFieldNames = getMatchedFieldNames(normalizedFieldName);
    return getMatchedFieldsByNames(matchedFields, matchedFieldNames);
};

export const useHasMatchedFieldByUrn = (urn: string, normalizedFieldName: NormalizedMatchedFieldName) => {
    const matchedFieldsForNormalizedField = useMatchedFieldsByNormalizedFieldName(normalizedFieldName);
    return getMatchedFieldByUrn(matchedFieldsForNormalizedField, urn);
};
