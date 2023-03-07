import React from 'react';
import styled from 'styled-components';
import { Popover, Tooltip } from 'antd';
import {
    ClockCircleOutlined,
    ConsoleSqlOutlined,
    TableOutlined,
    TeamOutlined,
    QuestionCircleOutlined,
    HddOutlined,
} from '@ant-design/icons';
import { formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';
import { ANTD_GRAY } from '../../shared/constants';
import { toLocalDateTimeString, toRelativeTimeString } from '../../../shared/time/timeUtils';
import { StatsSummary } from '../../shared/components/styled/StatsSummary';
import { FormattedBytesStat } from './FormattedBytesStat';

const StatText = styled.span<{ color: string }>`
    color: ${(props) => props.color};
`;

const HelpIcon = styled(QuestionCircleOutlined)<{ color: string }>`
    color: ${(props) => props.color};
    padding-left: 4px;
`;

type Props = {
    rowCount?: number | null;
    columnCount?: number | null;
    sizeInBytes?: number | null;
    queryCountLast30Days?: number | null;
    uniqueUserCountLast30Days?: number | null;
    lastUpdatedMs?: number | null;
    color?: string;
};

export const DatasetStatsSummary = ({
    rowCount,
    columnCount,
    sizeInBytes,
    queryCountLast30Days,
    uniqueUserCountLast30Days,
    lastUpdatedMs,
    color,
}: Props) => {
    const displayedColor = color !== undefined ? color : ANTD_GRAY[7];

    const statsViews = [
        !!rowCount && (
            <StatText color={displayedColor}>
                <TableOutlined style={{ marginRight: 8, color: displayedColor }} />
                <b>{formatNumberWithoutAbbreviation(rowCount)}</b> rows
                {!!columnCount && (
                    <>
                        , <b>{formatNumberWithoutAbbreviation(columnCount)}</b> columns
                    </>
                )}
            </StatText>
        ),
        !!sizeInBytes && (
            <StatText color={displayedColor}>
                <HddOutlined style={{ marginRight: 8, color: displayedColor }} />
                <FormattedBytesStat bytes={sizeInBytes} />
            </StatText>
        ),
        !!queryCountLast30Days && (
            <StatText color={displayedColor}>
                <ConsoleSqlOutlined style={{ marginRight: 8, color: displayedColor }} />
                <b>{formatNumberWithoutAbbreviation(queryCountLast30Days)}</b> queries last month
            </StatText>
        ),
        !!uniqueUserCountLast30Days && (
            <StatText color={displayedColor}>
                <TeamOutlined style={{ marginRight: 8, color: displayedColor }} />
                <b>{formatNumberWithoutAbbreviation(uniqueUserCountLast30Days)}</b> unique users
            </StatText>
        ),
        !!lastUpdatedMs && (
            <Popover
                content={
                    <div>
                        Changed on {toLocalDateTimeString(lastUpdatedMs)}.{' '}
                        <Tooltip title="The time at which the data was last changed in the source platform">
                            <HelpIcon color={displayedColor} />
                        </Tooltip>
                    </div>
                }
            >
                <StatText color={displayedColor}>
                    <ClockCircleOutlined style={{ marginRight: 8, color: displayedColor }} />
                    Changed {toRelativeTimeString(lastUpdatedMs)}
                </StatText>
            </Popover>
        ),
    ].filter((stat) => stat);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
