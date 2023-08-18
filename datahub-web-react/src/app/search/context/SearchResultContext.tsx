import React, { ReactNode, createContext, useContext, useMemo } from 'react';
import { SearchResult } from '../../../types.generated';
import {
    getMatchedFieldsByUrn,
    getMatchedFieldNames,
    getMatchedFieldsByNames,
    shouldShowInMatchedFieldList,
    getMatchedFieldLabel,
} from './utils';
import { MatchedFieldName } from './constants';

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

export const useSearchResult = () => {
    return useSearchResultContext()?.searchResult;
};

export const useEntityType = () => {
    return useSearchResultContext()?.searchResult.entity.type;
};

export const useMatchedFields = () => {
    return useSearchResult()?.matchedFields ?? [];
};

export const useMatchedFieldsForList = () => {
    const entityType = useEntityType();
    const matchedFields = useMatchedFields();
    return matchedFields.filter((field) => shouldShowInMatchedFieldList(entityType, field));
};

export const useMatchedFieldsByName = (fieldName: MatchedFieldName) => {
    const entityType = useEntityType();
    const matchedFields = useMatchedFields();
    const matchedFieldNames = getMatchedFieldNames(entityType, fieldName);
    return getMatchedFieldsByNames(matchedFields, matchedFieldNames);
};

export const useHasMatchedFieldByUrn = (urn: string, fieldName: MatchedFieldName) => {
    const matchedFieldsForNormalizedField = useMatchedFieldsByName(fieldName);
    return getMatchedFieldsByUrn(matchedFieldsForNormalizedField, urn).length > 0;
};

export const useMatchedFieldLabel = (fieldName: string) => {
    const entityType = useEntityType();
    return getMatchedFieldLabel(entityType, fieldName);
};
