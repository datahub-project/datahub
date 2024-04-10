import React from 'react';
import styled from 'styled-components';
import { useEntityData } from '../../../../../../../entity/shared/EntityContext';
import { SidebarHeaderSectionColumns } from '../../SidebarHeaderSectionColumns';
import SidebarPopularityHeaderSection from './SidebarPopularityHeaderSection';
import SidebarTopUsersHeaderSection from '../../shared/SidebarTopUsersHeaderSection';
import { isValuePresent, userExists } from '../../shared/utils';
import { REDESIGN_COLORS } from '../../../../../constants';
import { formatNumber } from '../../../../../../../shared/formatNumber';
import { getLastUpdatedMs } from '../../../../../../../entity/dataset/shared/utils';
import Freshness from '../../../../../../../previewV2/Freshness';

const StatContent = styled.div`
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_4};
    font-size: 12px;
    font-weight: 600;
`;

const SidebarDashboardHeaderSection = () => {
    const { entityData } = useEntityData();
    const dashboard = entityData as any;

    const columns: any = [];

    const lastUpdatedMs = getLastUpdatedMs(dashboard?.properties, (dashboard as any)?.lastOperation);

    /**
     * Popularity tab
     */
    if (
        isValuePresent(dashboard?.statsSummary?.viewCountPercentileLast30Days) &&
        isValuePresent(dashboard?.statsSummary?.uniqueUserPercentileLast30Days)
    ) {
        columns.push({
            title: 'Popularity',
            content: <SidebarPopularityHeaderSection />,
        });
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
    if (lastUpdatedMs) {
        columns.push({
            title: 'Freshness',
            content: <Freshness time={lastUpdatedMs} />,
        });
    }

    if (!columns.length) {
        return null;
    }

    return <SidebarHeaderSectionColumns columns={columns} />;
};

export default SidebarDashboardHeaderSection;
