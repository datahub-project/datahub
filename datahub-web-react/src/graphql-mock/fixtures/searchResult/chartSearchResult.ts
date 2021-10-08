import { Chart, SearchResult, SearchResults } from '../../../types.generated';
import { EntityBrowsePath } from '../../types';
import { filterEntityByPath } from '../browsePathHelper';
import { chartEntity } from '../entity/chartEntity';
import { generateData } from './dataGenerator';

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
