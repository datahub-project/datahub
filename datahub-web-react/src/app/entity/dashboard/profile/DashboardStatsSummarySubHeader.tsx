import React from 'react';

import { DashboardStatsSummary } from '@app/entity/dashboard/shared/DashboardStatsSummary';
import { useBaseEntity } from '@app/entity/shared/EntityContext';

import { GetDashboardQuery } from '@graphql/dashboard.generated';
import { DashboardStatsSummary as DashboardStatsSummaryObj } from '@types';

export const DashboardStatsSummarySubHeader = () => {
    const result = useBaseEntity<GetDashboardQuery>();
    const dashboard = result?.dashboard;
    const maybeStatsSummary = dashboard?.statsSummary as DashboardStatsSummaryObj;
    const chartCount = dashboard?.charts?.total;
    const viewCount = maybeStatsSummary?.viewCount;
    const uniqueUserCountLast30Days = maybeStatsSummary?.uniqueUserCountLast30Days;
    const lastUpdatedMs = dashboard?.properties?.lastModified?.time;
    const createdMs = dashboard?.properties?.created?.time;

    return (
        <DashboardStatsSummary
            chartCount={chartCount}
            viewCount={viewCount}
            uniqueUserCountLast30Days={uniqueUserCountLast30Days}
            lastUpdatedMs={lastUpdatedMs}
            createdMs={createdMs}
        />
    );
};
