import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { countFormatter, needsFormatting } from '../../../../utils/formatter';
import LastUpdated from '../../../shared/LastUpdated';
import { formatNumber, formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';
import { StatsSummary } from '../../shared/components/styled/StatsSummary';
import { ANTD_GRAY } from '../../shared/constants';
import { PercentileLabel } from '../../shared/stats/PercentileLabel';
import ExpandingStat from './ExpandingStat';

const StatText = styled.span<{ color: string }>`
    color: ${(props) => props.color};
    @media (min-width: 1160px) {
        white-space: nowrap;
    }
    font-size: 12px;
    font-family: 'Mulish', sans-serif;
    color: #8894a9;
`;

type Props = {
    rowCount?: number | null;
    columnCount?: number | null;
    sizeInBytes?: number | null;
    totalSqlQueries?: number | null;
    queryCountLast30Days?: number | null;
    queryCountPercentileLast30Days?: number | null;
    uniqueUserCountLast30Days?: number | null;
    uniqueUserPercentileLast30Days?: number | null;
    lastUpdatedMs?: number | null;
    color?: string;
    platformName?: string;
    platformLogoUrl?: string | null;
    subTypes?: string[];
    mode?: 'normal' | 'tooltip-content';
};

export const DatasetStatsSummary = ({
    rowCount,
    columnCount,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    sizeInBytes,
    totalSqlQueries,
    queryCountLast30Days,
    queryCountPercentileLast30Days,
    uniqueUserCountLast30Days,
    uniqueUserPercentileLast30Days,
    lastUpdatedMs,
    color,
    platformName,
    platformLogoUrl,
    subTypes,
    mode = 'normal',
}: Props) => {
    const isTooltipMode = mode === 'tooltip-content';
    const displayedColor = isTooltipMode ? '' : color ?? ANTD_GRAY[7];

    const statsViews = [
        !!rowCount && (
            <ExpandingStat
                disabled={isTooltipMode || !needsFormatting(rowCount)}
                render={(isExpanded) => (
                    <StatText color={displayedColor}>
                        {/* <TableOutlined style={{ marginRight: 8, color: displayedColor }} /> */}
                        {isExpanded ? formatNumberWithoutAbbreviation(rowCount) : countFormatter(rowCount)} rows
                        {!!columnCount && (
                            <>
                                ,{' '}
                                {isExpanded
                                    ? formatNumberWithoutAbbreviation(columnCount)
                                    : countFormatter(columnCount)}{' '}
                                columns
                            </>
                        )}
                    </StatText>
                )}
            />
        ),
        (!!queryCountLast30Days || !!totalSqlQueries) && (
            <StatText color={displayedColor}>
                {/* <ConsoleSqlOutlined style={{ marginRight: 8, color: displayedColor }} /> */}
                {formatNumber(queryCountLast30Days || totalSqlQueries)}{' '}
                {queryCountLast30Days ? <>queries</> : <>monthly queries</>}
                {!!queryCountPercentileLast30Days && (
                    <Typography.Text type="secondary">
                        <PercentileLabel
                            percentile={queryCountPercentileLast30Days}
                            description={`More queries than ${queryCountPercentileLast30Days}% of similar assets in the past 30 days`}
                        />
                    </Typography.Text>
                )}
            </StatText>
        ),
        !!uniqueUserCountLast30Days && (
            <StatText color={displayedColor}>
                {/* <TeamOutlined style={{ marginRight: 8, color: displayedColor }} /> */}
                {formatNumber(uniqueUserCountLast30Days)} users
                {!!uniqueUserPercentileLast30Days && (
                    <Typography.Text type="secondary">
                        <PercentileLabel
                            percentile={uniqueUserPercentileLast30Days}
                            description={`More users than ${uniqueUserPercentileLast30Days}% of similar assets in the past 30 days`}
                        />
                    </Typography.Text>
                )}
            </StatText>
        ),
        !!lastUpdatedMs && (
            <StatText color={displayedColor}>
                <LastUpdated
                    time={lastUpdatedMs}
                    platformName={platformName}
                    platformLogoUrl={platformLogoUrl || undefined}
                    typeName={subTypes?.[0] || 'dataset'}
                />
            </StatText>
        ),
    ].filter((stat) => stat);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
