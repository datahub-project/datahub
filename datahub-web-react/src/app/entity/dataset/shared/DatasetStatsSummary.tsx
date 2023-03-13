import React from 'react';
import styled from 'styled-components/macro';
import { Popover } from 'antd';
import { ClockCircleOutlined, ConsoleSqlOutlined, TableOutlined, TeamOutlined, HddOutlined } from '@ant-design/icons';
import { formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';
import { ANTD_GRAY } from '../../shared/constants';
import { toLocalDateTimeString, toRelativeTimeString } from '../../../shared/time/timeUtils';
import { StatsSummary } from '../../shared/components/styled/StatsSummary';
import { FormattedBytesStat } from './FormattedBytesStat';

const StatText = styled.span`
    color: ${ANTD_GRAY[8]};
`;

const PopoverContent = styled.div`
    max-width: 300px;
`;

type Props = {
    rowCount?: number | null;
    columnCount?: number | null;
    sizeInBytes?: number | null;
    queryCountLast30Days?: number | null;
    uniqueUserCountLast30Days?: number | null;
    lastUpdatedMs?: number | null;
};

export const DatasetStatsSummary = ({
    rowCount,
    columnCount,
    sizeInBytes,
    queryCountLast30Days,
    uniqueUserCountLast30Days,
    lastUpdatedMs,
}: Props) => {
    const statsViews = [
        !!rowCount && (
            <StatText>
                <TableOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                <b>{formatNumberWithoutAbbreviation(rowCount)}</b> rows
                {!!columnCount && (
                    <>
                        , <b>{formatNumberWithoutAbbreviation(columnCount)}</b> columns
                    </>
                )}
            </StatText>
        ),
        !!sizeInBytes && (
            <StatText>
                <HddOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                <FormattedBytesStat bytes={sizeInBytes} />
            </StatText>
        ),
        !!queryCountLast30Days && (
            <StatText>
                <ConsoleSqlOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                <b>{formatNumberWithoutAbbreviation(queryCountLast30Days)}</b> queries last month
            </StatText>
        ),
        !!uniqueUserCountLast30Days && (
            <StatText>
                <TeamOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                <b>{formatNumberWithoutAbbreviation(uniqueUserCountLast30Days)}</b> unique users
            </StatText>
        ),
        !!lastUpdatedMs && (
            <Popover
                content={
                    <PopoverContent>
                        Data was last updated in the source platform on{' '}
                        <strong>{toLocalDateTimeString(lastUpdatedMs)}</strong>
                    </PopoverContent>
                }
            >
                <StatText>
                    <ClockCircleOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                    Updated {toRelativeTimeString(lastUpdatedMs)}
                </StatText>
            </Popover>
        ),
    ].filter((stat) => stat);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
