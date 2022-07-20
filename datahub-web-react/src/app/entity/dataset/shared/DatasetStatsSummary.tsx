import React from 'react';
import styled from 'styled-components';
import { Popover, Tooltip, Typography } from 'antd';
import {
    ClockCircleOutlined,
    ConsoleSqlOutlined,
    TableOutlined,
    TeamOutlined,
    QuestionCircleOutlined,
} from '@ant-design/icons';
import { formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';
import { ANTD_GRAY } from '../../shared/constants';
import { toLocalDateTimeString, toRelativeTimeString } from '../../../shared/time/timeUtils';
import { StatsSummary } from '../../shared/components/styled/StatsSummary';
import { PercentileLabel } from '../../shared/stats/PercentileLabel';

const StatText = styled.span`
    color: ${ANTD_GRAY[8]};
`;

const HelpIcon = styled(QuestionCircleOutlined)`
    color: ${ANTD_GRAY[7]};
    padding-left: 4px;
`;

type Props = {
    rowCount?: number | null;
    queryCountLast30Days?: number | null;
    queryCountPercentileLast30Days?: number | null;
    uniqueUserCountLast30Days?: number | null;
    uniqueUserPercentileLast30Days?: number | null;
    lastUpdatedMs?: number | null;
};

export const DatasetStatsSummary = ({
    rowCount,
    queryCountLast30Days,
    queryCountPercentileLast30Days,
    uniqueUserCountLast30Days,
    uniqueUserPercentileLast30Days,
    lastUpdatedMs,
}: Props) => {
    const statsViews = [
        (!!rowCount && (
            <StatText>
                <TableOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                <b>{formatNumberWithoutAbbreviation(rowCount)}</b> rows
            </StatText>
        )) ||
            undefined,
        (!!queryCountLast30Days && (
            <StatText>
                <ConsoleSqlOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                <b>{formatNumberWithoutAbbreviation(queryCountLast30Days)}</b> queries last month
                {!!queryCountPercentileLast30Days && (
                    <Typography.Text type="secondary">
                        -{' '}
                        <PercentileLabel
                            percentile={queryCountPercentileLast30Days}
                            description={`This dataset has been queried more often than ${queryCountPercentileLast30Days}% of similar datasets in the past 30 days.`}
                        />
                    </Typography.Text>
                )}
            </StatText>
        )) ||
            undefined,
        (!!uniqueUserCountLast30Days && (
            <StatText>
                <TeamOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                <b>{formatNumberWithoutAbbreviation(uniqueUserCountLast30Days)}</b> unique users
                {!!uniqueUserPercentileLast30Days && (
                    <Typography.Text type="secondary">
                        -{' '}
                        <PercentileLabel
                            percentile={uniqueUserPercentileLast30Days}
                            description={`This dataset has had more unique users than ${uniqueUserPercentileLast30Days}% of similar datasets in the past 30 days.`}
                        />
                    </Typography.Text>
                )}
            </StatText>
        )) ||
            undefined,
        (!!lastUpdatedMs && (
            <Popover
                content={
                    <div>
                        Changed on {toLocalDateTimeString(lastUpdatedMs)}.{' '}
                        <Tooltip title="The time at which the data was last changed in the source platform">
                            <HelpIcon />
                        </Tooltip>
                    </div>
                }
            >
                <StatText>
                    <ClockCircleOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                    Changed {toRelativeTimeString(lastUpdatedMs)}
                </StatText>
            </Popover>
        )) ||
            undefined,
    ].filter((stat) => stat !== undefined);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
