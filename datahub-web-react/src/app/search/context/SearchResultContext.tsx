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

const useEntityType = () => {
    return useSearchResultContext()?.searchResult.entity.type;
};

const useMatchedFields = () => {
    return useSearchResultContext()?.searchResult.matchedFields;
};

export const useMatchedFieldsByNormalizedFieldName = (normalizedFieldName?: NormalizedMatchedFieldName) => {
    const entityType = useEntityType();
    const matchedFields = useMatchedFields();
    const matchedFieldNames = getMatchedFieldNames(entityType, normalizedFieldName);
    return getMatchedFieldsByNames(matchedFields, matchedFieldNames);
};

export const useHasMatchedFieldByUrn = (urn: string, normalizedFieldName: NormalizedMatchedFieldName) => {
    const matchedFieldsForNormalizedField = useMatchedFieldsByNormalizedFieldName(normalizedFieldName);
    return getMatchedFieldByUrn(matchedFieldsForNormalizedField, urn);
};
