import * as fixtures from '../fixtures';
import {
    Chart,
    CorpUser,
    Dashboard,
    DataFlow,
    DataJob,
    Dataset,
    EntityType,
    SearchInput,
    SearchResult,
    SearchResults,
} from '../../types.generated';

const entitySearchResults = {
    [EntityType.Dataset]: fixtures.datasetSearchResult,
    [EntityType.Dashboard]: fixtures.dashboardSearchResult,
    [EntityType.Chart]: fixtures.chartSearchResult,
    [EntityType.CorpUser]: fixtures.userSearchResult,
    [EntityType.DataFlow]: fixtures.dataFlowSearchResult,
    [EntityType.DataJob]: fixtures.dataJobSearchResult,
};

type FindByQueryArg = {
    query: string;
    searchResults: SearchResult[];
    start: number;
    count: number;
};

type QueryResult = {
    results: SearchResult[];
    total: number;
};

const findByQuery = ({ query, searchResults, start, count }: FindByQueryArg): QueryResult => {
    if (query === '*') {
        const results = searchResults.slice(start, start + count);

        return { results, total: results.length };
    }

    if (query.indexOf('owners:') >= 0) {
        const [, ownerQuery] = query.split(':');
        const results = searchResults.filter((r) => {
            return (r.entity as Dataset | Dashboard | Chart | DataFlow | DataJob).ownership?.owners?.filter((o) => {
                return (o.owner as CorpUser).username === ownerQuery;
            }).length;
        });

        return { results, total: results.length };
    }

    if (query.indexOf('tags:') >= 0) {
        const [, tagQuery] = query.split(':');
        const results = searchResults.filter((r) => {
            return (r.entity as Dataset | Dashboard | Chart | DataFlow | DataJob).globalTags?.tags?.filter((t) => {
                return t.tag.name === tagQuery;
            }).length;
        });

        return { results, total: results.length };
    }

    const results = [
        ...searchResults.filter((r) => {
            return ((r.entity as Dataset).name?.indexOf(query) ?? -1) >= 0;
        }),
        ...searchResults.filter((r) => {
            return ((r.entity as Dashboard | Chart | DataFlow | DataJob).info?.name?.indexOf(query) ?? -1) >= 0;
        }),
        ...searchResults.filter((r) => {
            return ((r.entity as CorpUser).info?.fullName?.toLowerCase()?.indexOf(query) ?? -1) >= 0;
        }),
    ];

    return { results, total: results.length };
};

type GetSearchResults = {
    data: {
        search: SearchResults;
    };
} | null;

export const getSearchResultsResolver = {
    getSearchResults({ variables: { input } }): GetSearchResults {
        const { type, start = 0, count = 10, query }: SearchInput = input;
        const startValue = start as number;
        const countValue = count as number;
        const entitySearchResult: SearchResults = entitySearchResults[type];
        const { results, total } = findByQuery({
            query: query.toLowerCase(),
            searchResults: entitySearchResult.searchResults,
            start: startValue,
            count: countValue,
        });

        return entitySearchResult
            ? {
                  data: {
                      search: {
                          start: startValue,
                          count: results.length,
                          total,
                          searchResults: results,
                          facets: entitySearchResult.facets,
                          __typename: entitySearchResult.__typename,
                      },
                  },
              }
            : null;
    },
};
