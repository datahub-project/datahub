import { ClockCircleOutlined, EyeOutlined, QuestionCircleOutlined, TeamOutlined } from '@ant-design/icons';
import { Popover, Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import ExpandingStat from '@app/entityV2/dataset/shared/ExpandingStat';
import { StatsSummary } from '@app/entityV2/shared/components/styled/StatsSummary';
import { PercentileLabel } from '@app/entityV2/shared/stats/PercentileLabel';
import { formatNumber, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { toLocalDateTimeString, toRelativeTimeString } from '@app/shared/time/timeUtils';
import { countFormatter, needsFormatting } from '@utils/formatter';

const StatText = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
`;

const HelpIcon = styled(QuestionCircleOutlined)`
    color: ${(props) => props.theme.colors.textTertiary};
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
    const { t } = useTranslation('entity.types');
    const theme = useTheme();
    // acryl-main only.
    const effectiveViewCount = (!!viewCountLast30Days && viewCountLast30Days) || viewCount;

    const statsViews = [
        (!!chartCount && (
            <ExpandingStat
                disabled={!needsFormatting(chartCount)}
                render={(isExpanded) => (
                    <StatText>
                        <Trans
                            t={t}
                            i18nKey="shared.chartsCount"
                            count={chartCount}
                            values={{
                                count: chartCount,
                                formattedCount: isExpanded
                                    ? formatNumberWithoutAbbreviation(chartCount)
                                    : countFormatter(chartCount),
                            }}
                            components={{ bold: <b /> }}
                        />
                    </StatText>
                )}
            />
        )) ||
            undefined,
        (!!effectiveViewCount && (
            <StatText>
                <EyeOutlined style={{ marginRight: 8, color: theme.colors.textTertiary }} />
                {viewCountLast30Days
                    ? t('shared.viewsLast30DaysCount', {
                          count: effectiveViewCount,
                          formattedCount: formatNumber(effectiveViewCount),
                      })
                    : t('shared.viewsCount', {
                          count: effectiveViewCount,
                          formattedCount: formatNumber(effectiveViewCount),
                      })}
                {!!viewCountPercentileLast30Days && (
                    <PercentileLabel
                        percentile={viewCountPercentileLast30Days}
                        description={t('shared.morePopularViewsPercentile', {
                            percentile: viewCountPercentileLast30Days,
                        })}
                    />
                )}
            </StatText>
        )) ||
            undefined,
        (!!uniqueUserCountLast30Days && (
            <StatText>
                <TeamOutlined style={{ marginRight: 8, color: theme.colors.textTertiary }} />
                {t('shared.usersCount', {
                    count: uniqueUserCountLast30Days,
                    formattedCount: formatNumber(uniqueUserCountLast30Days),
                })}
                {!!uniqueUserPercentileLast30Days && (
                    <Typography.Text type="secondary">
                        <PercentileLabel
                            percentile={uniqueUserPercentileLast30Days}
                            description={t('shared.morePopularUsersPercentile', {
                                percentile: uniqueUserPercentileLast30Days,
                            })}
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
                        {createdMs && (
                            <div>{t('shared.createdOnDate', { date: toLocalDateTimeString(createdMs) })}</div>
                        )}
                        <div>
                            {t('shared.changedOnDate', { date: toLocalDateTimeString(lastUpdatedMs) })}{' '}
                            <Tooltip title={t('chart.lastChangedTooltip')}>
                                <HelpIcon />
                            </Tooltip>
                        </div>
                    </>
                }
            >
                <StatText>
                    <ClockCircleOutlined style={{ marginRight: 8, color: theme.colors.textTertiary }} />
                    {t('shared.changedRelativeTime', { time: toRelativeTimeString(lastUpdatedMs) })}
                </StatText>
            </Popover>
        )) ||
            undefined,
    ].filter((stat) => stat !== undefined);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
