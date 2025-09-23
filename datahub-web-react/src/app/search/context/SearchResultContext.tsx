import React, { ReactNode, createContext, useContext, useMemo } from 'react';

import { MatchedFieldName } from '@app/search/matches/constants';
import {
    getMatchedFieldLabel,
    getMatchedFieldNames,
    getMatchedFieldsByNames,
    getMatchedFieldsByUrn,
    getMatchesPrioritized,
    shouldShowInMatchedFieldList,
} from '@app/search/matches/utils';

import { EntityType, MatchedField, SearchResult } from '@types';

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
    return useSearchResultContext()?.searchResult?.entity?.type;
};

export const useMatchedFields = () => {
    return useSearchResult()?.matchedFields ?? [];
};

export function getMatchedFieldsForList(primaryField: string, entityType: EntityType, matchedFields: MatchedField[]) {
    const showableFields = matchedFields.filter((field) => shouldShowInMatchedFieldList(entityType, field));
    return getMatchesPrioritized(entityType, showableFields, primaryField);
}

export const useMatchedFieldsForList = (primaryField: MatchedFieldName) => {
    const entityType = useEntityType();
    const matchedFields = useMatchedFields();
    const showableFields = matchedFields.filter((field) => shouldShowInMatchedFieldList(entityType, field));
    return entityType ? getMatchesPrioritized(entityType, showableFields, primaryField) : [];
};

export const useMatchedFieldsByGroup = (fieldName: MatchedFieldName) => {
    const entityType = useEntityType();
    const matchedFields = useMatchedFields();
    const matchedFieldNames = getMatchedFieldNames(entityType, fieldName);
    return getMatchedFieldsByNames(matchedFields, matchedFieldNames);
};

export const useHasMatchedFieldByUrn = (urn: string, fieldName: MatchedFieldName) => {
    const matchedFields = useMatchedFieldsByGroup(fieldName);
    return getMatchedFieldsByUrn(matchedFields, urn).length > 0;
};

export const useMatchedFieldLabel = (fieldName: string) => {
    const entityType = useEntityType();
    return getMatchedFieldLabel(entityType, fieldName);
};
