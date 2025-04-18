import { Chart } from '../../types.generated';
import { findChartByURN } from '../fixtures/searchResult/chartSearchResult';

type GetChart = {
    data: {
        chart: Chart;
    };
};

export const getChartResolver = {
    getChart({ variables: { urn } }): GetChart {
        const chart = findChartByURN(urn) as Chart;
        return {
            data: {
                chart: Object.assign(chart, {
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
