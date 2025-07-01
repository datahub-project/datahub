import { dataJobEntity } from '@graphql-mock/fixtures/entity/dataJobEntity';
import { generateData } from '@graphql-mock/fixtures/searchResult/dataGenerator';
import { DataJob, SearchResult, SearchResults } from '@types';

const searchResult = (): SearchResult => {
    return {
        entity: dataJobEntity(),
        matchedFields: [],
        __typename: 'SearchResult',
    };
};

const generateSearchResults = (): SearchResult[] => {
    return generateData<SearchResult>({ generator: searchResult, count: 2 });
};

const searchResults = generateSearchResults();

export const dataJobSearchResult: SearchResults = {
    start: 0,
    count: 0,
    total: 0,
    searchResults,
    facets: [],
    __typename: 'SearchResults',
};

export const findDataJobByURN = (urn: string): DataJob => {
    return searchResults.find((r) => {
        return (r.entity as DataJob).urn === urn;
    })?.entity as DataJob;
};
