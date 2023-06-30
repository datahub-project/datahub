import React from 'react';
import { ANTD_GRAY } from '../../shared/constants';
import { StatsSummary } from '../../shared/components/styled/StatsSummary';
import ChartCountStat from '../../shared/components/styled/stat/ChartCountStat';
import ViewCountStat from '../../shared/components/styled/stat/ViewCountStat';
import UserCountStat from '../../shared/components/styled/stat/UserCountStat';
import LastUpdatedStat from '../../shared/components/styled/stat/LastUpdatedStat';

type Props = {
    chartCount?: number | null;
    viewCount?: number | null;
    uniqueUserCountLast30Days?: number | null;
    lastUpdatedMs?: number | null;
    createdMs?: number | null;
};

export const ChartStatsSummary = ({
    chartCount,
    viewCount,
    uniqueUserCountLast30Days,
    lastUpdatedMs,
    createdMs,
}: Props) => {
    const color = ANTD_GRAY[7];

    const statsViews = [
        !!chartCount && <ChartCountStat color={color} chartCount={chartCount} />,
        !!viewCount && <ViewCountStat color={color} viewCount={viewCount} />,
        !!uniqueUserCountLast30Days && <UserCountStat color={color} userCount={uniqueUserCountLast30Days} />,
        !!lastUpdatedMs && <LastUpdatedStat color={color} lastUpdatedMs={lastUpdatedMs} createdMs={createdMs} />,
    ].filter(Boolean);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
