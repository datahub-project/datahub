/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { findChartByURN } from '@graphql-mock/fixtures/searchResult/chartSearchResult';
import { updateEntityOwners, updateEntityTag } from '@graphql-mock/mutationHelper';
import { Chart, ChartUpdateInput } from '@types';

type UpdateChart = {
    data: { updateChart: Chart };
};

export const updateChartResolver = {
    updateChart({ variables: { urn, input } }): UpdateChart {
        const { globalTags, ownership }: ChartUpdateInput = input;
        const chart = findChartByURN(urn);

        if (ownership) {
            updateEntityOwners({ entity: chart, owners: ownership?.owners });
        } else if (globalTags) {
            updateEntityTag({ entity: chart, globalTags });
        }

        return {
            data: {
                updateChart: Object.assign(chart, {
                    info: {
                        ...chart.info,
                        inputs: [],
                        customProperties: [],
                        lastRefreshed: null,
                        created: {
                            time: 1619160920,
                            __typename: 'AuditStamp',
                        },
                    },
                    query: null,
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
