import { Dashboard, SearchResult, SearchResults } from '../../../types.generated';
import { EntityBrowsePath } from '../../types';
import { filterEntityByPath } from '../browsePathHelper';
import { dashboardEntity } from '../entity/dashboardEntity';
import { generateData } from './dataGenerator';

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
