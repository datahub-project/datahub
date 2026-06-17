import { ClockCircleOutlined, ConsoleSqlOutlined, HddOutlined, TableOutlined, TeamOutlined } from '@ant-design/icons';
import { Popover } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import ExpandingStat from '@app/entity/dataset/shared/ExpandingStat';
import { FormattedBytesStat } from '@app/entity/dataset/shared/FormattedBytesStat';
import { StatsSummary } from '@app/entity/shared/components/styled/StatsSummary';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { toLocalDateTimeString, toRelativeTimeString } from '@app/shared/time/timeUtils';
import { countFormatter, needsFormatting } from '@utils/formatter';

const StatText = styled.span<{ color: string }>`
    color: ${(props) => props.color};
    @media (min-width: 1160px) {
        white-space: nowrap;
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
    shouldWrap?: boolean;
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
    shouldWrap,
}: Props) => {
    const { t } = useTranslation('entity.types');
    const { t: tt } = useTranslation('shared.time');
    const isTooltipMode = mode === 'tooltip-content';
    const displayedColor = isTooltipMode ? '' : (color ?? ANTD_GRAY[7]);

    const statsViews = [
        !!rowCount && (
            <ExpandingStat
                disabled={isTooltipMode || !needsFormatting(rowCount)}
                render={(isExpanded) => (
                    <StatText color={displayedColor}>
                        <TableOutlined style={{ marginRight: 8, color: displayedColor }} />
                        {columnCount
                            ? t('dataset.rowsColumnsCount', {
                                  count: rowCount,
                                  formattedCount: isExpanded
                                      ? formatNumberWithoutAbbreviation(rowCount)
                                      : countFormatter(rowCount),
                                  formattedColumnCount: isExpanded
                                      ? formatNumberWithoutAbbreviation(columnCount)
                                      : countFormatter(columnCount),
                              })
                            : t('dataset.rowsCount', {
                                  count: rowCount,
                                  formattedCount: isExpanded
                                      ? formatNumberWithoutAbbreviation(rowCount)
                                      : countFormatter(rowCount),
                              })}
                    </StatText>
                )}
            />
        ),
        !!sizeInBytes && (
            <StatText color={displayedColor}>
                <HddOutlined style={{ marginRight: 8, color: displayedColor }} />
                <FormattedBytesStat bytes={sizeInBytes} />
            </StatText>
        ),
        (!!queryCountLast30Days || !!totalSqlQueries) && (
            <StatText color={displayedColor}>
                <ConsoleSqlOutlined style={{ marginRight: 8, color: displayedColor }} />
                {queryCountLast30Days
                    ? t('dataset.queriesLastMonthCount', {
                          count: queryCountLast30Days,
                          formattedCount: formatNumberWithoutAbbreviation(queryCountLast30Days),
                      })
                    : t('dataset.monthlyQueriesCount', {
                          count: totalSqlQueries ?? 0,
                          formattedCount: formatNumberWithoutAbbreviation(totalSqlQueries),
                      })}
            </StatText>
        ),
        !!uniqueUserCountLast30Days && (
            <StatText color={displayedColor}>
                <TeamOutlined style={{ marginRight: 8, color: displayedColor }} />
                {t('dataset.uniqueUsersCount', {
                    count: uniqueUserCountLast30Days,
                    formattedCount: formatNumberWithoutAbbreviation(uniqueUserCountLast30Days),
                })}
            </StatText>
        ),
        !!lastUpdatedMs && (
            <Popover
                content={
                    <PopoverContent>
                        <Trans
                            t={t}
                            i18nKey="dataset.lastUpdatedPopover"
                            values={{ date: toLocalDateTimeString(lastUpdatedMs) }}
                            components={{ strong: <strong /> }}
                        />
                    </PopoverContent>
                }
            >
                <StatText color={displayedColor}>
                    <ClockCircleOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                    {tt('lastUpdated.updatedRelative', { relativeTime: toRelativeTimeString(lastUpdatedMs) })}
                </StatText>
            </Popover>
        ),
    ].filter((stat) => stat);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} shouldWrap={shouldWrap} />}</>;
};
