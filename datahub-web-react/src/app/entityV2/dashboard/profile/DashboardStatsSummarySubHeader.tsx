/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { DashboardStatsSummary } from '@app/entityV2/dashboard/shared/DashboardStatsSummary';

import { GetDashboardQuery } from '@graphql/dashboard.generated';
import { DashboardStatsSummary as DashboardStatsSummaryObj } from '@types';

export const DashboardStatsSummarySubHeader = () => {
    const result = useBaseEntity<GetDashboardQuery>();
    const dashboard = result?.dashboard;
    const maybeStatsSummary = dashboard?.statsSummary as DashboardStatsSummaryObj;
    const chartCount = dashboard?.charts?.total;
    const viewCount = maybeStatsSummary?.viewCount;
    const viewCountLast30Days = maybeStatsSummary?.viewCountLast30Days;
    const uniqueUserCountLast30Days = maybeStatsSummary?.uniqueUserCountLast30Days;
    const lastUpdatedMs = dashboard?.properties?.lastModified?.time;
    const createdMs = dashboard?.properties?.created?.time;

    return (
        <DashboardStatsSummary
            chartCount={chartCount}
            viewCount={viewCount}
            viewCountLast30Days={viewCountLast30Days}
            uniqueUserCountLast30Days={uniqueUserCountLast30Days}
            lastUpdatedMs={lastUpdatedMs}
            createdMs={createdMs}
        />
    );
};
