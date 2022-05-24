import {
    AutoCompleteInput,
    AutoCompleteResults,
    Chart,
    CorpUser,
    Dashboard,
    DataFlow,
    DataJob,
    Dataset,
    EntityType,
} from '../../types.generated';
import * as fixtures from '../fixtures';
import { tagDb } from '../fixtures/tag';

type GetAutoCompleteResults = {
    data: {
        autoComplete: AutoCompleteResults;
    };
};

const findSuggestions = ({ query, type, field }: AutoCompleteInput): string[] => {
    const q = query.toLowerCase().trim();

    if (type === EntityType.Tag) {
        const results = q
            ? tagDb.filter((t) => {
                  return t.name.indexOf(q) >= 0;
              })
            : [];
        return results.map((r) => {
            return r.name;
        });
    }

    const filterSearchResults = () => {
        if (field === 'ldap') {
            return fixtures.userSearchResult.searchResults.filter((r) => {
                return ((r.entity as CorpUser).username?.indexOf(q) ?? -1) >= 0;
            });
        }
        return [
            ...fixtures.datasetSearchResult.searchResults.filter((r) => {
                return ((r.entity as Dataset).name?.indexOf(q) ?? -1) >= 0;
            }),
            ...fixtures.dashboardSearchResult.searchResults.filter((r) => {
                return ((r.entity as Dashboard).info?.name?.indexOf(q) ?? -1) >= 0;
            }),
            ...fixtures.chartSearchResult.searchResults.filter((r) => {
                return ((r.entity as Chart).info?.name?.indexOf(q) ?? -1) >= 0;
            }),
            ...fixtures.dataFlowSearchResult.searchResults.filter((r) => {
                return ((r.entity as DataFlow).info?.name?.indexOf(q) ?? -1) >= 0;
            }),
            ...fixtures.dataJobSearchResult.searchResults.filter((r) => {
                return ((r.entity as DataJob).info?.name?.indexOf(q) ?? -1) >= 0;
            }),
            ...fixtures.userSearchResult.searchResults.filter((r) => {
                return ((r.entity as CorpUser).info?.fullName?.toLowerCase()?.indexOf(q) ?? -1) >= 0;
            }),
        ];
    };

    const results = q ? filterSearchResults() : [];

    return results
        .map((r) => {
            return field === 'ldap'
                ? (r.entity as CorpUser)?.username
                : (r.entity as Dataset)?.name ||
                      (r.entity as Dashboard | Chart | DataFlow | DataJob)?.info?.name ||
                      (r.entity as CorpUser)?.info?.fullName ||
                      '';
        })
        .filter(Boolean);
};

export const getAutoCompleteResultsResolver = {
    getAutoCompleteResults({ variables: { input } }): GetAutoCompleteResults {
        const { query }: AutoCompleteInput = input;
        const suggestions = findSuggestions(input);

        return {
            data: {
                autoComplete: {
                    query,
                    suggestions: suggestions.filter((s, i) => {
                        return suggestions.indexOf(s) === i;
                    }),
                    entities: [],
                    __typename: 'AutoCompleteResults',
                },
            },
        };
    },
};
