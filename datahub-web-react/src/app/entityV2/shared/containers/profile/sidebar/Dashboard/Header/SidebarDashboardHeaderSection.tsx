import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { SidebarHeaderSectionColumns } from '@app/entityV2/shared/containers/profile/sidebar/SidebarHeaderSectionColumns';
import SidebarTopUsersHeaderSection from '@app/entityV2/shared/containers/profile/sidebar/shared/SidebarTopUsersHeaderSection';
import { getDashboardPopularityTier, userExists } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { SidebarStatsColumn, getPopularityColumn } from '@app/entityV2/shared/containers/profile/utils';
import { formatNumber, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

const StatContent = styled.div`
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_4};
    font-size: 12px;
    font-weight: 600;
`;

const SidebarDashboardHeaderSection = () => {
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
            title: 'Top Users',
            content: <SidebarTopUsersHeaderSection />,
        });
    }

    /**
     * Queries column
     */
    if (dashboard?.statsSummary?.queryCountLast30Days) {
        columns.push({
            title: 'Queries',
            content: <StatContent>{formatNumber(dashboard?.statsSummary?.queryCountLast30Days)} queries</StatContent>,
        });
    }

    /**
     * Users column
     */
    if (dashboard?.statsSummary?.uniqueUserCountLast30Days) {
        columns.push({
            title: 'Users',
            content: (
                <StatContent>{formatNumber(dashboard?.statsSummary?.uniqueUserCountLast30Days)} users</StatContent>
            ),
        });
    }

    /**
     * Recent Views
     */
    if (dashboard?.statsSummary?.viewCountLast30Days) {
        columns.push({
            title: 'Recent Views',
            content: (
                <Tooltip
                    showArrow={false}
                    title={`${formatNumberWithoutAbbreviation(
                        dashboard?.statsSummary?.viewCountLast30Days,
                    )} views over the past 30 days`}
                >
                    <StatContent>{formatNumber(dashboard?.statsSummary?.viewCountLast30Days)} views</StatContent>
                </Tooltip>
            ),
        });
    }

    /**
     * Total Views
     */
    if (dashboard?.statsSummary?.viewCount) {
        columns.push({
            title: 'Total Views',
            content: (
                <Tooltip
                    showArrow={false}
                    title={`${formatNumberWithoutAbbreviation(dashboard?.statsSummary?.viewCount)} views`}
                >
                    <StatContent>{formatNumber(dashboard?.statsSummary?.viewCount)} views</StatContent>
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
