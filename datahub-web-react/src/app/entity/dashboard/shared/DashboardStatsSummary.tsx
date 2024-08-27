import React from 'react';
import styled from 'styled-components';
import { Popover, Tooltip } from 'antd';
import { ClockCircleOutlined, EyeOutlined, TeamOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import { useTranslation } from 'react-i18next';
import { formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';
import { ANTD_GRAY } from '../../shared/constants';
import { toLocalDateTimeString, toRelativeTimeString } from '../../../shared/time/timeUtils';
import { StatsSummary } from '../../shared/components/styled/StatsSummary';
import { countFormatter, needsFormatting } from '../../../../utils/formatter';
import ExpandingStat from '../../dataset/shared/ExpandingStat';

const StatText = styled.span`
    color: ${ANTD_GRAY[8]};
    @media (min-width: 1024px) {
        white-space: nowrap;
`;

const HelpIcon = styled(QuestionCircleOutlined)`
    color: ${ANTD_GRAY[7]};
    padding-left: 4px;
`;

type Props = {
    chartCount?: number | null;
    viewCount?: number | null;
    uniqueUserCountLast30Days?: number | null;
    lastUpdatedMs?: number | null;
    createdMs?: number | null;
};

export const DashboardStatsSummary = ({
    chartCount,
    viewCount,
    uniqueUserCountLast30Days,
    lastUpdatedMs,
    createdMs,
}: Props) => {
    const { t } = useTranslation();
    const statsViews = [
        (!!chartCount && (
            <ExpandingStat
                disabled={!needsFormatting(chartCount)}
                render={(isExpanded) => (
                    <StatText color={ANTD_GRAY[8]}>
                        <b>{isExpanded ? formatNumberWithoutAbbreviation(chartCount) : countFormatter(chartCount)}</b>{' '}
                        charts
                    </StatText>
                )}
            />
        )) ||
            undefined,
        (!!viewCount && (
            <StatText>
                <EyeOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                <b>{formatNumberWithoutAbbreviation(viewCount)}</b> {t('common.ciews')}
            </StatText>
        )) ||
            undefined,
        (!!uniqueUserCountLast30Days && (
            <StatText>
                <TeamOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                <b>{formatNumberWithoutAbbreviation(uniqueUserCountLast30Days)}</b> {t('common.uniqueUsers')}
            </StatText>
        )) ||
            undefined,
        (!!lastUpdatedMs && (
            <Popover
                content={
                    <>
                        {createdMs && (
                            <div>
                                {t('reporting.createdOnWithDate')} {toLocalDateTimeString(createdMs)}.
                            </div>
                        )}
                        <div>
                            {t('common.changed')} {toLocalDateTimeString(lastUpdatedMs)}.{' '}
                            <Tooltip title={t('chart.lastChangedTimeInSource')}>
                                <HelpIcon />
                            </Tooltip>
                        </div>
                    </>
                }
            >
                <StatText>
                    <ClockCircleOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                    {t('common.changed')} {toRelativeTimeString(lastUpdatedMs)}
                </StatText>
            </Popover>
        )) ||
            undefined,
    ].filter((stat) => stat !== undefined);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
