import React from 'react';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { ChartStatsSummary } from '@app/entityV2/chart/shared/ChartStatsSummary';

import { GetChartQuery } from '@graphql/chart.generated';
import { ChartStatsSummary as ChartStatsSummaryObj } from '@types';

export const ChartStatsSummarySubHeader = () => {
    const result = useBaseEntity<GetChartQuery>();
    const chart = result?.chart;
    const maybeStatsSummary = chart?.statsSummary as ChartStatsSummaryObj;
    const viewCount = maybeStatsSummary?.viewCount;
    const uniqueUserCountLast30Days = maybeStatsSummary?.uniqueUserCountLast30Days;
    const lastUpdatedMs = chart?.properties?.lastModified?.time;
    const createdMs = chart?.properties?.created?.time;
    const viewCountLast30Days = maybeStatsSummary?.viewCountLast30Days;

    return (
        <ChartStatsSummary
            viewCount={viewCount}
            viewCountLast30Days={viewCountLast30Days}
            uniqueUserCountLast30Days={uniqueUserCountLast30Days}
            lastUpdatedMs={lastUpdatedMs}
            createdMs={createdMs}
        />
    );
};
