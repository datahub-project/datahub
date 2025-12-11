/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { filterEntityByPath } from '@graphql-mock/fixtures/browsePathHelper';
import { chartEntity } from '@graphql-mock/fixtures/entity/chartEntity';
import { generateData } from '@graphql-mock/fixtures/searchResult/dataGenerator';
import { EntityBrowsePath } from '@graphql-mock/types';
import { Chart, SearchResult, SearchResults } from '@types';

const searchResult = (tool: string) => (): SearchResult => {
    return {
        entity: chartEntity(tool),
        matchedFields: [],
        __typename: 'SearchResult',
    };
};

export const chartBrowsePaths: EntityBrowsePath[] = [{ name: 'superset', paths: [], count: 6 }];

const generateSearchResults = (): SearchResult[] => {
    return chartBrowsePaths.flatMap(({ name, count = 0 }) => {
        return generateData<SearchResult>({ generator: searchResult(name), count });
    });
};

const searchResults = generateSearchResults();

export const chartSearchResult: SearchResults = {
    start: 0,
    count: 0,
    total: 0,
    searchResults,
    facets: [
        { field: 'access', displayName: 'access', aggregations: [], __typename: 'FacetMetadata' },
        {
            field: 'type',
            displayName: 'type',
            aggregations: [
                { value: 'TABLE', count: 1, __typename: 'AggregationMetadata' },
                { value: 'BAR', count: 3, __typename: 'AggregationMetadata' },
                { value: 'PIE', count: 1, __typename: 'AggregationMetadata' },
                { value: 'LINE', count: 1, __typename: 'AggregationMetadata' },
            ],
            __typename: 'FacetMetadata',
        },
        {
            field: 'tool',
            displayName: 'tool',
            aggregations: [{ value: 'superset', count: 6, __typename: 'AggregationMetadata' }],
            __typename: 'FacetMetadata',
        },
        { field: 'queryType', displayName: 'queryType', aggregations: [], __typename: 'FacetMetadata' },
    ],
    __typename: 'SearchResults',
};

export const filterChartByTool = (tool: string): Chart[] => {
    return searchResults
        .filter((r) => {
            return (r.entity as Chart).tool === tool;
        })
        .map((r) => r.entity as Chart);
};

export const findChartByURN = (urn: string): Chart => {
    return searchResults.find((r) => {
        return (r.entity as Chart).urn === urn;
    })?.entity as Chart;
};

export const filterChartByPath = (path: string[]): Chart[] => {
    return filterEntityByPath({ term: path.slice(-2).join('.'), searchResults }) as Chart[];
};
