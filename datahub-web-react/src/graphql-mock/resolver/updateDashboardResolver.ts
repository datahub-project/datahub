import { findDashboardByURN } from '@graphql-mock/fixtures/searchResult/dashboardSearchResult';
import { updateEntityOwners, updateEntityTag } from '@graphql-mock/mutationHelper';
import { Dashboard, DashboardUpdateInput } from '@types';

type UpdateDashboard = {
    data: { updateDashboard: Dashboard };
};

export const updateDashboardResolver = {
    updateDashboard({ variables: { urn, input } }): UpdateDashboard {
        const { ownership, globalTags }: DashboardUpdateInput = input;
        const dashboard = findDashboardByURN(urn);

        if (ownership) {
            updateEntityOwners({ entity: dashboard, owners: ownership?.owners });
        } else if (globalTags) {
            updateEntityTag({ entity: dashboard, globalTags });
        }

        return {
            data: {
                updateDashboard: Object.assign(dashboard, {
                    info: {
                        ...dashboard.info,
                        charts: [],
                        customProperties: [],
                        lastRefreshed: null,
                        created: {
                            time: 1619160920,
                            __typename: 'AuditStamp',
                        },
                    },
                    downstreamLineage: {
                        entities: [],
                        __typename: 'DownstreamEntityRelationships',
                    },
                    upstreamLineage: {
                        entities: [],
                        __typename: 'UpstreamEntityRelationships',
                    },
                }),
            },
        };
    },
};
