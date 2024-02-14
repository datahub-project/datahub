import React from 'react';
import { DashboardStatsSummary as DashboardStatsSummaryObj } from '../../../../types.generated';
import { useBaseEntity } from '../../shared/EntityContext';
import { GetDashboardQuery } from '../../../../graphql/dashboard.generated';
import { DashboardStatsSummary } from '../shared/DashboardStatsSummary';

export const DashboardStatsSummarySubHeader = () => {
    const result = useBaseEntity<GetDashboardQuery>();
    const dashboard = result?.dashboard;
    const maybeStatsSummary = dashboard?.statsSummary as DashboardStatsSummaryObj;
    const chartCount = dashboard?.charts?.total;
    const viewCount = maybeStatsSummary?.viewCount;
    const viewCountLast30Days = maybeStatsSummary?.viewCountLast30Days;
    const viewCountPercentileLast30Days = maybeStatsSummary?.viewCountPercentileLast30Days;
    const uniqueUserCountLast30Days = maybeStatsSummary?.uniqueUserCountLast30Days;
    const uniqueUserPercentileLast30Days = maybeStatsSummary?.uniqueUserPercentileLast30Days;
    const lastUpdatedMs = dashboard?.properties?.lastModified?.time;
    const createdMs = dashboard?.properties?.created?.time;

    return (
        <DashboardStatsSummary
            chartCount={chartCount}
            viewCount={viewCount}
            viewCountLast30Days={viewCountLast30Days}
            viewCountPercentileLast30Days={viewCountPercentileLast30Days}
            uniqueUserCountLast30Days={uniqueUserCountLast30Days}
            uniqueUserPercentileLast30Days={uniqueUserPercentileLast30Days}
            lastUpdatedMs={lastUpdatedMs}
            createdMs={createdMs}
        />
    );
};
