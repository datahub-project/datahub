/* eslint-disable no-param-reassign */
import React from 'react';
import styled from 'styled-components/macro';
import { Popover } from 'antd';
import { ClockCircleOutlined, ConsoleSqlOutlined, TableOutlined, TeamOutlined, HddOutlined } from '@ant-design/icons';
import { formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';
import { ANTD_GRAY } from '../../shared/constants';
import { toLocalDateTimeString, toRelativeTimeString } from '../../../shared/time/timeUtils';
import { StatsSummary } from '../../shared/components/styled/StatsSummary';
import { FormattedBytesStat } from './FormattedBytesStat';
import { countFormatter, needsFormatting } from '../../../../utils/formatter';
import ExpandingStat from './ExpandingStat';

const StatText = styled.span<{ color: string }>`
    color: ${(props) => props.color};
`;

const PopoverContent = styled.div`
    max-width: 300px;
`;

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
    rowCount = 2133440;
    columnCount = 12;
    sizeInBytes = 29321728;
    totalSqlQueries = 987654321;
    queryCountLast30Days = 987654321;
    uniqueUserCountLast30Days = 98765;
    lastUpdatedMs = Date.now();

    const isTooltipMode = mode === 'tooltip-content';
    const displayedColor = isTooltipMode ? '' : color ?? ANTD_GRAY[7];

    const statsViews = [
        !!rowCount && (
            <ExpandingStat
                disabled={isTooltipMode || !needsFormatting(rowCount)}
                render={(isExpanded) => (
                    <StatText color={displayedColor}>
                        <TableOutlined style={{ marginRight: 8, color: displayedColor }} />
                        <b>
                            {isExpanded
                                ? formatNumberWithoutAbbreviation(rowCount)
                                : countFormatter(rowCount as number)}
                        </b>{' '}
                        rows
                        {!!columnCount && (
                            <>
                                ,{' '}
                                <b>
                                    {isExpanded
                                        ? formatNumberWithoutAbbreviation(columnCount)
                                        : countFormatter(columnCount)}
                                </b>{' '}
                                columns
                            </>
                        )}
                    </StatText>
                )}
            />
        ),
        !!sizeInBytes && (
            <StatText color={displayedColor}>
                <HddOutlined style={{ marginRight: 8, color: displayedColor }} />
                <FormattedBytesStat bytes={sizeInBytes} disableTooltip={isTooltipMode} />
            </StatText>
        ),
        (!!queryCountLast30Days || !!totalSqlQueries) && (
            <StatText color={displayedColor}>
                <ConsoleSqlOutlined style={{ marginRight: 8, color: displayedColor }} />
                <b>{formatNumberWithoutAbbreviation(queryCountLast30Days || totalSqlQueries)}</b>{' '}
                {queryCountLast30Days ? <>queries last month</> : <>monthly queries</>}
            </StatText>
        ),
        !!uniqueUserCountLast30Days && (
            <StatText color={displayedColor}>
                <TeamOutlined style={{ marginRight: 8, color: displayedColor }} />
                <b>{formatNumberWithoutAbbreviation(uniqueUserCountLast30Days)}</b> unique users - high
            </StatText>
        ),
        !!lastUpdatedMs && (
            <Popover
                open={isTooltipMode ? false : undefined}
                content={
                    <PopoverContent>
                        Data was last updated in the source platform on{' '}
                        <strong>{toLocalDateTimeString(lastUpdatedMs)}</strong>
                    </PopoverContent>
                }
            >
                <StatText color={displayedColor}>
                    <ClockCircleOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                    Updated {toRelativeTimeString(lastUpdatedMs)}
                </StatText>
            </Popover>
        ),
    ].filter((stat) => stat);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
