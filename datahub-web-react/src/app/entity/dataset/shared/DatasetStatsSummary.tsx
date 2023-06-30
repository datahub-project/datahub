import React from 'react';
import { ANTD_GRAY } from '../../shared/constants';
import { StatsSummary } from '../../shared/components/styled/StatsSummary';
import RowCountStat from '../../shared/components/styled/stat/RowCountStat';
import ByteCountStat from '../../shared/components/styled/stat/ByteCountStat';
import QueryCountStat from '../../shared/components/styled/stat/QueryCountStat';
import UserCountStat from '../../shared/components/styled/stat/UserCountStat';
import LastUpdatedStat from '../../shared/components/styled/stat/LastUpdatedStat';

type Props = {
    rowCount?: number | null;
    columnCount?: number | null;
    sizeInBytes?: number | null;
    totalSqlQueries?: number | null;
    queryCountLast30Days?: number | null;
    uniqueUserCountLast30Days?: number | null;
    lastUpdatedMs?: number | null;
    color?: string;
    mode?: 'normal' | 'tooltip-content';
};

export const DatasetStatsSummary = ({
    rowCount,
    columnCount,
    sizeInBytes,
    totalSqlQueries,
    queryCountLast30Days,
    uniqueUserCountLast30Days,
    lastUpdatedMs,
    color,
    mode = 'normal',
}: Props) => {
    const isTooltipMode = mode === 'tooltip-content';
    const displayedColor = isTooltipMode ? '' : color ?? ANTD_GRAY[7];

    const statsViews = [
        !!rowCount && (
            <RowCountStat
                color={displayedColor}
                disabled={isTooltipMode}
                rowCount={rowCount}
                columnCount={columnCount}
            />
        ),
        !!sizeInBytes && <ByteCountStat color={displayedColor} disabled={isTooltipMode} sizeInBytes={sizeInBytes} />,
        (!!queryCountLast30Days || !!totalSqlQueries) && (
            <QueryCountStat
                color={displayedColor}
                disabled={isTooltipMode}
                queryCountLast30Days={queryCountLast30Days}
                totalSqlQueries={totalSqlQueries}
            />
        ),
        !!uniqueUserCountLast30Days && (
            <UserCountStat color={displayedColor} disabled={isTooltipMode} userCount={uniqueUserCountLast30Days} />
        ),
        !!lastUpdatedMs && (
            <LastUpdatedStat color={displayedColor} disabled={isTooltipMode} lastUpdatedMs={lastUpdatedMs} />
        ),
    ].filter(Boolean);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
