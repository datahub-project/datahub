/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
