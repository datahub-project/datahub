import React from 'react';

import { ChartStatsSummary } from '@app/entity/chart/shared/ChartStatsSummary';
import { useBaseEntity } from '@app/entity/shared/EntityContext';

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

    return (
        <ChartStatsSummary
            viewCount={viewCount}
            uniqueUserCountLast30Days={uniqueUserCountLast30Days}
            lastUpdatedMs={lastUpdatedMs}
            createdMs={createdMs}
        />
    );
};
