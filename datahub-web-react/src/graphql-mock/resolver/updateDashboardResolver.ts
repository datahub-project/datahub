/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
