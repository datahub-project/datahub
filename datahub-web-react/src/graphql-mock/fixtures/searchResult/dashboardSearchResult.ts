/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { filterEntityByPath } from '@graphql-mock/fixtures/browsePathHelper';
import { dashboardEntity } from '@graphql-mock/fixtures/entity/dashboardEntity';
import { generateData } from '@graphql-mock/fixtures/searchResult/dataGenerator';
import { EntityBrowsePath } from '@graphql-mock/types';
import { Dashboard, SearchResult, SearchResults } from '@types';

const searchResult = (tool: string) => (): SearchResult => {
    return {
        entity: dashboardEntity(tool),
        matchedFields: [],
        __typename: 'SearchResult',
    };
};

export const dashboardBrowsePaths: EntityBrowsePath[] = [{ name: 'superset', paths: [], count: 3 }];

const generateSearchResults = (): SearchResult[] => {
    return dashboardBrowsePaths.flatMap(({ name, count = 0 }) => {
        return generateData<SearchResult>({ generator: searchResult(name), count });
    });
};

const searchResults = generateSearchResults();

export const dashboardSearchResult: SearchResults = {
    start: 0,
    count: 0,
    total: 0,
    searchResults,
    facets: [
        {
            field: 'tool',
            displayName: 'tool',
            aggregations: [{ value: 'superset', count: 1, __typename: 'AggregationMetadata' }],
            __typename: 'FacetMetadata',
        },
        {
            field: 'access',
            displayName: 'access',
            aggregations: [],
            __typename: 'FacetMetadata',
        },
    ],
    __typename: 'SearchResults',
};

export const filterDashboardByTool = (tool: string): Dashboard[] => {
    return searchResults
        .filter((r) => {
            return (r.entity as Dashboard).tool === tool;
        })
        .map((r) => r.entity as Dashboard);
};

export const findDashboardByURN = (urn: string): Dashboard => {
    return searchResults.find((r) => {
        return (r.entity as Dashboard).urn === urn;
    })?.entity as Dashboard;
};

export const filterDashboardByPath = (path: string[]): Dashboard[] => {
    return filterEntityByPath({ term: path.slice(-2).join('.'), searchResults }) as Dashboard[];
};
