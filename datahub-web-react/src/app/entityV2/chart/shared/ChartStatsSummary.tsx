import { Icon, Popover, Text, Tooltip } from '@components';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Eye } from '@phosphor-icons/react/dist/csr/Eye';
import { Question } from '@phosphor-icons/react/dist/csr/Question';
import { UsersThree } from '@phosphor-icons/react/dist/csr/UsersThree';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import ExpandingStat from '@app/entityV2/dataset/shared/ExpandingStat';
import { StatsSummary } from '@app/entityV2/shared/components/styled/StatsSummary';
import { PercentileLabel } from '@app/entityV2/shared/stats/PercentileLabel';
import { formatNumber, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { toLocalDateTimeString, toRelativeTimeString } from '@app/shared/time/timeUtils';
import { countFormatter, needsFormatting } from '@utils/formatter';

const StatText = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
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
                <Icon icon={Eye} color="textTertiary" style={{ marginRight: 8 }} />
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
                <Icon icon={UsersThree} color="textTertiary" style={{ marginRight: 8 }} />
                {t('shared.usersCount', {
                    count: uniqueUserCountLast30Days,
                    formattedCount: formatNumber(uniqueUserCountLast30Days),
                })}
                {!!uniqueUserPercentileLast30Days && (
                    <Text type="span" color="textSecondary">
                        <PercentileLabel
                            percentile={uniqueUserPercentileLast30Days}
                            description={t('shared.morePopularUsersPercentile', {
                                percentile: uniqueUserPercentileLast30Days,
                            })}
                        />
                    </Text>
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
                                <Icon icon={Question} color="textTertiary" style={{ paddingLeft: 4 }} />
                            </Tooltip>
                        </div>
                    </>
                }
            >
                <StatText>
                    <Icon icon={Clock} color="textTertiary" style={{ marginRight: 8 }} />
                    {t('shared.changedRelativeTime', { time: toRelativeTimeString(lastUpdatedMs) })}
                </StatText>
            </Popover>
        )) ||
            undefined,
    ].filter((stat) => stat !== undefined);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
