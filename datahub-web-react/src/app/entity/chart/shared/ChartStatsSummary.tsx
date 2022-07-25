import React from 'react';
import styled from 'styled-components';
import { Popover, Tooltip, Typography } from 'antd';
import { ClockCircleOutlined, EyeOutlined, TeamOutlined, QuestionCircleOutlined } from '@ant-design/icons';
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
    chartCount?: number | null;
    viewCount?: number | null;
    viewCountLast30Days?: number | null;
    viewCountPercentileLast30Days?: number | null;
    uniqueUserCountLast30Days?: number | null;
    uniqueUserPercentileLast30Days?: number | null;
    lastUpdatedMs?: number | null;
    createdMs?: number | null;
};

export const ChartStatsSummary = ({
    chartCount,
    viewCount,
    viewCountLast30Days,
    viewCountPercentileLast30Days,
    uniqueUserCountLast30Days,
    uniqueUserPercentileLast30Days,
    lastUpdatedMs,
    createdMs,
}: Props) => {
    // acryl-main only.
    const effectiveViewCount = (!!viewCountLast30Days && viewCountLast30Days) || viewCount;
    const effectiveViewCountText = (!!viewCountLast30Days && 'views last month') || 'views';

    const statsViews = [
        (!!chartCount && (
            <StatText>
                <b>{chartCount}</b> charts
            </StatText>
        )) ||
            undefined,
        (!!effectiveViewCount && (
            <StatText>
                <EyeOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                <b>{formatNumberWithoutAbbreviation(effectiveViewCount)}</b> {effectiveViewCountText}
                {!!viewCountPercentileLast30Days && (
                    <Typography.Text type="secondary">
                        -{' '}
                        <PercentileLabel
                            percentile={viewCountPercentileLast30Days}
                            description={`This chart has been viewed more often than ${viewCountPercentileLast30Days}% of similar charts in the past 30 days.`}
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
                            description={`This chart has had more unique users than ${uniqueUserPercentileLast30Days}% of similar charts in the past 30 days.`}
                        />
                    </Typography.Text>
                )}
            </StatText>
        )) ||
            undefined,
        (!!lastUpdatedMs && (
            <Popover
                content={
                    <>
                        {createdMs && <div>Created on {toLocalDateTimeString(createdMs)}.</div>}
                        <div>
                            Changed on {toLocalDateTimeString(lastUpdatedMs)}.{' '}
                            <Tooltip title="The time at which the chart was last changed in the source platform">
                                <HelpIcon />
                            </Tooltip>
                        </div>
                    </>
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
