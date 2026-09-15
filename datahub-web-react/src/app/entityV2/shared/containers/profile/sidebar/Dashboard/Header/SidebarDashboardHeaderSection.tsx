import { Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SidebarHeaderSectionColumns } from '@app/entityV2/shared/containers/profile/sidebar/SidebarHeaderSectionColumns';
import SidebarTopUsersHeaderSection from '@app/entityV2/shared/containers/profile/sidebar/shared/SidebarTopUsersHeaderSection';
import { getDashboardPopularityTier, userExists } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { SidebarStatsColumn, getPopularityColumn } from '@app/entityV2/shared/containers/profile/utils';
import { formatNumber, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

const StatContent = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    font-weight: 600;
`;

const SidebarDashboardHeaderSection = () => {
    const { t } = useTranslation('entity.shared.containers');
    const { entityData } = useEntityData();
    const dashboard = entityData as any;

    const columns: SidebarStatsColumn[] = [];

    /**
     * Popularity tab
     */
    const popularityColumn = getPopularityColumn(
        getDashboardPopularityTier(
            dashboard?.statsSummary?.viewCountPercentileLast30Days,
            dashboard?.statsSummary?.uniqueUserPercentileLast30Days,
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
        dashboard?.statsSummary?.topUsersLast30Days &&
        dashboard?.statsSummary?.topUsersLast30Days?.find((user) => userExists(user))
    ) {
        columns.push({
            title: t('sidebar.dashboardHeader.topUsersColumn'),
            content: <SidebarTopUsersHeaderSection />,
        });
    }

    /**
     * Queries column
     */
    if (dashboard?.statsSummary?.queryCountLast30Days) {
        const count = dashboard?.statsSummary?.queryCountLast30Days;
        columns.push({
            title: t('sidebar.dashboardHeader.queriesColumn'),
            content: (
                <StatContent>
                    {t('sidebar.dashboardHeader.queriesCount', { count, formattedCount: formatNumber(count) })}
                </StatContent>
            ),
        });
    }

    /**
     * Users column
     */
    if (dashboard?.statsSummary?.uniqueUserCountLast30Days) {
        const count = dashboard?.statsSummary?.uniqueUserCountLast30Days;
        columns.push({
            title: t('sidebar.dashboardHeader.usersColumn'),
            content: (
                <StatContent>
                    {t('sidebar.dashboardHeader.usersCount', { count, formattedCount: formatNumber(count) })}
                </StatContent>
            ),
        });
    }

    /**
     * Recent Views
     */
    if (dashboard?.statsSummary?.viewCountLast30Days) {
        const count = dashboard?.statsSummary?.viewCountLast30Days;
        columns.push({
            title: t('sidebar.dashboardHeader.recentViewsColumn'),
            content: (
                <Tooltip
                    showArrow={false}
                    title={t('sidebar.dashboardHeader.viewsOverPast30DaysTooltip', {
                        count,
                        formattedCount: formatNumberWithoutAbbreviation(count),
                    })}
                >
                    <StatContent>
                        {t('sidebar.dashboardHeader.viewsCount', { count, formattedCount: formatNumber(count) })}
                    </StatContent>
                </Tooltip>
            ),
        });
    }

    /**
     * Total Views
     */
    if (dashboard?.statsSummary?.viewCount) {
        const count = dashboard?.statsSummary?.viewCount;
        columns.push({
            title: t('sidebar.dashboardHeader.totalViewsColumn'),
            content: (
                <Tooltip
                    showArrow={false}
                    title={t('sidebar.dashboardHeader.totalViewsTooltip', {
                        count,
                        formattedCount: formatNumberWithoutAbbreviation(count),
                    })}
                >
                    <StatContent>
                        {t('sidebar.dashboardHeader.viewsCount', { count, formattedCount: formatNumber(count) })}
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

export default SidebarDashboardHeaderSection;
