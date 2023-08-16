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

export const useIsSearchResult = () => {
    return !!useSearchResultContext();
};

export const useEntityType = () => {
    return useSearchResultContext()?.searchResult.entity.type;
};

const useMatchedFields = () => {
    return useSearchResultContext()?.searchResult.matchedFields;
};

// todo - what if we have both description and editedDescription? how do we pick
// do we need to do a .filter instead of a .find or something like that?
export const useMatchedFieldsByNormalizedFieldName = (normalizedFieldName?: NormalizedMatchedFieldName) => {
    const entityType = useEntityType();
    const matchedFields = useMatchedFields();
    const matchedFieldNames = getMatchedFieldNames(entityType, normalizedFieldName);
    return matchedFields?.filter((field) => matchedFieldNames.includes(field.name));
};

export const useMatchedFieldByUrn = (urn: string, normalizedFieldName: NormalizedMatchedFieldName) => {
    const matchedFieldsForNormalizedField = useMatchedFieldsByNormalizedFieldName(normalizedFieldName);
    return matchedFieldsForNormalizedField?.some((field) => field.value === urn);
};
