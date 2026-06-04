import { Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';
import styled from 'styled-components/macro';

import ExpandingStat from '@app/entityV2/dataset/shared/ExpandingStat';
import { StatsSummary } from '@app/entityV2/shared/components/styled/StatsSummary';
import { PercentileLabel } from '@app/entityV2/shared/stats/PercentileLabel';
import LastUpdated from '@app/shared/LastUpdated';
import { formatNumber, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { countFormatter, needsFormatting } from '@utils/formatter';

const StatText = styled.span<{ color: string }>`
    color: ${(props) => props.color};
    @media (min-width: 1160px) {
        white-space: nowrap;
    }
    font-size: 12px;
    font-family: 'Mulish', sans-serif;
    color: ${(props) => props.theme.colors.textTertiary};
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
    const { t } = useTranslation('entity.types');
    const theme = useTheme();
    const isTooltipMode = mode === 'tooltip-content';
    const displayedColor = isTooltipMode ? '' : (color ?? theme.colors.textTertiary);

    const statsViews = [
        !!rowCount && (
            <ExpandingStat
                disabled={isTooltipMode || !needsFormatting(rowCount)}
                render={(isExpanded) => {
                    const formatCount = (value: number) =>
                        isExpanded ? formatNumberWithoutAbbreviation(value) : countFormatter(value);
                    return (
                        <StatText color={displayedColor}>
                            {/* <TableOutlined style={{ marginRight: 8, color: displayedColor }} /> */}
                            {t('dataset.rowsColumnsCount', {
                                count: rowCount,
                                formattedCount: formatCount(rowCount),
                                formattedColumnCount: columnCount ? formatCount(columnCount) : '',
                            })}
                        </StatText>
                    );
                }}
            />
        ),
        (!!queryCountLast30Days || !!totalSqlQueries) && (
            <StatText color={displayedColor}>
                {/* <ConsoleSqlOutlined style={{ marginRight: 8, color: displayedColor }} /> */}
                {queryCountLast30Days
                    ? t('dataset.queriesCount', {
                          count: queryCountLast30Days,
                          formattedCount: formatNumber(queryCountLast30Days),
                      })
                    : t('dataset.monthlyQueriesCount', {
                          count: totalSqlQueries ?? 0,
                          formattedCount: formatNumber(totalSqlQueries),
                      })}
                {!!queryCountPercentileLast30Days && (
                    <Typography.Text type="secondary">
                        <PercentileLabel
                            percentile={queryCountPercentileLast30Days}
                            description={t('dataset.moreQueriesPercentile', {
                                percentile: queryCountPercentileLast30Days,
                            })}
                        />
                    </Typography.Text>
                )}
            </StatText>
        ),
        !!uniqueUserCountLast30Days && (
            <StatText color={displayedColor}>
                {/* <TeamOutlined style={{ marginRight: 8, color: displayedColor }} /> */}
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
        ),
        !!lastUpdatedMs && (
            <StatText color={displayedColor}>
                <LastUpdated
                    time={lastUpdatedMs}
                    platformName={platformName}
                    platformLogoUrl={platformLogoUrl || undefined}
                    // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) programmatic subtype identifier fallback, not user-facing copy
                    typeName={subTypes?.[0] || 'dataset'}
                />
            </StatText>
        ),
    ].filter((stat) => stat);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
