import {
    AutoCompleteMultipleResults,
    AutoCompleteInput,
    AutoCompleteResultForEntity,
    Chart,
    CorpUser,
    Dashboard,
    DataFlow,
    DataJob,
    Dataset,
    EntityType,
    Maybe,
} from '../../types.generated';
import * as fixtures from '../fixtures';
import { tagDb } from '../fixtures/tag';

type GetAutoCompleteAllResults = {
    data: {
        autoCompleteForAll: AutoCompleteMultipleResults;
    };
};

const findSuggestions = ({ query, type }: AutoCompleteInput): AutoCompleteResultForEntity[] => {
    const q = query.toLowerCase().trim();

    if (type === EntityType.Tag) {
        const results = q
            ? tagDb.filter((t) => {
                  return t.name.indexOf(q) >= 0;
              })
            : [];
        return [
            {
                type: EntityType.Tag,
                suggestions: results.map((r) => {
                    return r.name;
                }),
                entities: results.map((r) => {
                    return r;
                }),
                __typename: 'AutoCompleteResultForEntity',
            },
        ];
    }

    const filterSearchResults = (): AutoCompleteResultForEntity[] => {
        const datasetResults = fixtures.datasetSearchResult.searchResults.filter((r) => {
            return ((r.entity as Dataset).name?.toLowerCase()?.indexOf(q) ?? -1) >= 0;
        });
        const datasetQueryResults: Maybe<AutoCompleteResultForEntity> = datasetResults.length
            ? {
                  type: EntityType.Dataset,
                  suggestions: datasetResults.map((r) => {
                      return (r.entity as Dataset).name;
                  }),
                  entities: [],
                  __typename: 'AutoCompleteResultForEntity',
              }
            : null;

        const dashboardResults = fixtures.dashboardSearchResult.searchResults.filter((r) => {
            return ((r.entity as Dashboard).info?.name?.toLowerCase()?.indexOf(q) ?? -1) >= 0;
        });
        const dashboardQueryResults: Maybe<AutoCompleteResultForEntity> = dashboardResults.length
            ? {
                  type: EntityType.Dashboard,
                  suggestions: dashboardResults.map((r) => {
                      return (r.entity as Dashboard).info?.name || '';
                  }),
                  entities: dashboardResults.map((r) => {
                      return r.entity;
                  }),
                  __typename: 'AutoCompleteResultForEntity',
              }
            : null;

        const chartResults = fixtures.chartSearchResult.searchResults.filter((r) => {
            return ((r.entity as Chart).info?.name?.toLowerCase()?.indexOf(q) ?? -1) >= 0;
        });
        const chartQueryResults: Maybe<AutoCompleteResultForEntity> = chartResults.length
            ? {
                  type: EntityType.Chart,
                  suggestions: chartResults.map((r) => {
                      return (r.entity as Chart).info?.name || '';
                  }),
                  entities: chartResults.map((r) => {
                      return r.entity;
                  }),
                  __typename: 'AutoCompleteResultForEntity',
              }
            : null;

        const dataFlowResults = fixtures.dataFlowSearchResult.searchResults.filter((r) => {
            return ((r.entity as DataFlow).info?.name?.toLowerCase()?.indexOf(q) ?? -1) >= 0;
        });
        const dataFlowQueryResults: Maybe<AutoCompleteResultForEntity> = dataFlowResults.length
            ? {
                  type: EntityType.DataFlow,
                  suggestions: dataFlowResults.map((r) => {
                      return (r.entity as DataFlow).info?.name || '';
                  }),
                  entities: dataFlowResults.map((r) => {
                      return r.entity;
                  }),
                  __typename: 'AutoCompleteResultForEntity',
              }
            : null;

        const dataJobResults = fixtures.dataJobSearchResult.searchResults.filter((r) => {
            return ((r.entity as DataJob).info?.name?.toLowerCase()?.indexOf(q) ?? -1) >= 0;
        });
        const dataJobQueryResults: Maybe<AutoCompleteResultForEntity> = dataJobResults.length
            ? {
                  type: EntityType.DataJob,
                  suggestions: dataJobResults.map((r) => {
                      return (r.entity as DataJob).info?.name || '';
                  }),
                  entities: dataJobResults.map((r) => {
                      return r.entity;
                  }),
                  __typename: 'AutoCompleteResultForEntity',
              }
            : null;

        const userResults = fixtures.userSearchResult.searchResults.filter((r) => {
            return ((r.entity as CorpUser).info?.fullName?.toLowerCase()?.indexOf(q) ?? -1) >= 0;
        });
        const userQueryResults: Maybe<AutoCompleteResultForEntity> = userResults.length
            ? {
                  type: EntityType.CorpUser,
                  suggestions: userResults.map((r) => {
                      return (r.entity as CorpUser).info?.fullName || '';
                  }),
                  entities: userResults.map((r) => {
                      return r.entity;
                  }),
                  __typename: 'AutoCompleteResultForEntity',
              }
            : null;

        return [
            datasetQueryResults,
            dashboardQueryResults,
            chartQueryResults,
            dataFlowQueryResults,
            dataJobQueryResults,
            userQueryResults,
        ].filter(Boolean) as AutoCompleteResultForEntity[];
    };

    return q ? filterSearchResults() : [];
};

export const getAutoCompleteAllResultsResolver = {
    getAutoCompleteAllResults({ variables: { input } }): GetAutoCompleteAllResults {
        const { query }: AutoCompleteInput = input;
        const suggestions = findSuggestions(input);

        return {
            data: {
                autoCompleteForAll: {
                    query,
                    suggestions: suggestions.filter((s, i) => {
                        return suggestions.indexOf(s) === i;
                    }),
                    __typename: 'AutoCompleteMultipleResults',
                },
            },
        };
    },
};
