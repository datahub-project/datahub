import { Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SidebarHeaderSectionColumns } from '@app/entityV2/shared/containers/profile/sidebar/SidebarHeaderSectionColumns';
import SidebarTopUsersHeaderSection from '@app/entityV2/shared/containers/profile/sidebar/shared/SidebarTopUsersHeaderSection';
import { getChartPopularityTier, userExists } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { SidebarStatsColumn, getPopularityColumn } from '@app/entityV2/shared/containers/profile/utils';
import { formatNumber, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

const StatContent = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    font-weight: 600;
`;

const SidebarChartHeaderSection = () => {
    const { t } = useTranslation('entity.shared.containers');
    const { entityData } = useEntityData();
    const chart = entityData as any;

    const columns: SidebarStatsColumn[] = [];

    /**
     * Popularity tab
     */
    const popularityColumn = getPopularityColumn(
        getChartPopularityTier(
            chart?.statsSummary?.viewCountPercentileLast30Days,
            chart?.statsSummary?.uniqueUserPercentileLast30Days,
        ),
        t,
    );
    if (popularityColumn) {
        columns.push(popularityColumn);
    }

    /**
     * Top users tab
     */
    if (
        chart?.statsSummary?.topUsersLast30Days &&
        chart?.statsSummary?.topUsersLast30Days?.find((user) => userExists(user))
    ) {
        columns.push({
            title: t('sidebar.chartHeader.topUsersColumn'),
            content: <SidebarTopUsersHeaderSection />,
        });
    }

    /**
     * Queries column
     */
    if (chart?.statsSummary?.queryCountLast30Days) {
        const count = chart?.statsSummary?.queryCountLast30Days;
        columns.push({
            title: t('sidebar.chartHeader.queriesColumn'),
            content: (
                <StatContent>
                    {t('sidebar.chartHeader.queriesCount', { count, formattedCount: formatNumber(count) })}
                </StatContent>
            ),
        });
    }

    /**
     * Users column
     */
    if (chart?.statsSummary?.uniqueUserCountLast30Days) {
        const count = chart?.statsSummary?.uniqueUserCountLast30Days;
        columns.push({
            title: t('sidebar.chartHeader.usersColumn'),
            content: (
                <StatContent>
                    {t('sidebar.chartHeader.usersCount', { count, formattedCount: formatNumber(count) })}
                </StatContent>
            ),
        });
    }

    /**
     * Recent Views
     */
    if (chart?.statsSummary?.viewCountLast30Days) {
        const count = chart?.statsSummary?.viewCountLast30Days;
        columns.push({
            title: t('sidebar.chartHeader.recentViewsColumn'),
            content: (
                <Tooltip
                    showArrow={false}
                    title={t('sidebar.chartHeader.viewsOverPast30DaysTooltip', {
                        count,
                        formattedCount: formatNumberWithoutAbbreviation(count),
                    })}
                >
                    <StatContent>
                        {t('sidebar.chartHeader.viewsCount', { count, formattedCount: formatNumber(count) })}
                    </StatContent>
                </Tooltip>
            ),
        });
    }

    /**
     * Total Views
     */
    if (chart?.statsSummary?.viewCount) {
        const count = chart?.statsSummary?.viewCount;
        columns.push({
            title: t('sidebar.chartHeader.totalViewsColumn'),
            content: (
                <Tooltip
                    showArrow={false}
                    title={t('sidebar.chartHeader.totalViewsTooltip', {
                        count,
                        formattedCount: formatNumberWithoutAbbreviation(count),
                    })}
                >
                    <StatContent>
                        {t('sidebar.chartHeader.viewsCount', { count, formattedCount: formatNumber(count) })}
                    </StatContent>
                </Tooltip>
            ),
        });
    }

    if (!columns.length) {
        return null;
    }

    return <SidebarHeaderSectionColumns columns={columns} />;
};

export default SidebarChartHeaderSection;
