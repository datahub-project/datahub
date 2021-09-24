import { Dashboard, DashboardUpdateInput } from '../../types.generated';
import { findDashboardByURN } from '../fixtures/searchResult/dashboardSearchResult';
import { updateEntityOwners, updateEntityTag } from '../mutationHelper';

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
