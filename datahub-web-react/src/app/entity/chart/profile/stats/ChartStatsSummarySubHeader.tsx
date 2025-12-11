/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
