import { Chart, ChartUpdateInput } from '../../types.generated';
import { findChartByURN } from '../fixtures/searchResult/chartSearchResult';
import { updateEntityOwners, updateEntityTag } from '../mutationHelper';

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
