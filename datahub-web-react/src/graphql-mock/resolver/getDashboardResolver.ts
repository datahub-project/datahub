import { findDashboardByURN } from '@graphql-mock/fixtures/searchResult/dashboardSearchResult';
import { Dashboard } from '@types';

type GetDashboard = {
    data: { dashboard: Dashboard };
};

export const getDashboardResolver = {
    getDashboard({ variables: { urn } }): GetDashboard {
        const dashboard = findDashboardByURN(urn) as Dashboard;
        return {
            data: {
                dashboard: Object.assign(dashboard, {
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
