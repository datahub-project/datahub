import React from 'react';
import styled from 'styled-components';
import { useEntityData } from '../../../../../../../entity/shared/EntityContext';
import { SidebarHeaderSectionColumns } from '../../SidebarHeaderSectionColumns';
import SidebarTopUsersHeaderSection from '../../shared/SidebarTopUsersHeaderSection';
import { getDashboardPopularityTier, userExists } from '../../shared/utils';
import { REDESIGN_COLORS } from '../../../../../constants';
import { formatNumber } from '../../../../../../../shared/formatNumber';
import { getDashboardLastUpdatedMs } from '../../../../../utils';
import Freshness, { getFreshnessTitle } from '../../../../../../../previewV2/Freshness';
import { getPopularityColumn, SidebarStatsColumn } from '../../../utils';

const StatContent = styled.div`
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_4};
    font-size: 12px;
    font-weight: 600;
`;

const SidebarDashboardHeaderSection = () => {
    const { entityData } = useEntityData();
    const dashboard = entityData as any;

    const columns: SidebarStatsColumn[] = [];

    const timeData = getDashboardLastUpdatedMs(dashboard?.properties);

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
     * Freshness column
     */
    if (timeData?.lastUpdatedMs) {
        columns.push({
            title: getFreshnessTitle(timeData?.property),
            content: <Freshness time={timeData?.lastUpdatedMs} timeProperty={timeData?.property} />,
        });
    }

    if (!columns.length) {
        return null;
    }

    return <SidebarHeaderSectionColumns columns={columns} />;
};

export default SidebarDashboardHeaderSection;
