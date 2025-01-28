import React from 'react';
import styled from 'styled-components';
import { Tooltip } from '@components';
import { useEntityData } from '../../../../../../../entity/shared/EntityContext';
import { SidebarHeaderSectionColumns } from '../../SidebarHeaderSectionColumns';
import SidebarTopUsersHeaderSection from '../../shared/SidebarTopUsersHeaderSection';
import { getChartPopularityTier, userExists } from '../../shared/utils';
import { REDESIGN_COLORS } from '../../../../../constants';
import { formatNumber, formatNumberWithoutAbbreviation } from '../../../../../../../shared/formatNumber';
import { getPopularityColumn, SidebarStatsColumn } from '../../../utils';

const StatContent = styled.div`
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_4};
    font-size: 12px;
    font-weight: 600;
`;

const SidebarChartHeaderSection = () => {
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
            title: 'Top Users',
            content: <SidebarTopUsersHeaderSection />,
        });
    }

    /**
     * Queries column
     */
    if (chart?.statsSummary?.queryCountLast30Days) {
        columns.push({
            title: 'Queries',
            content: <StatContent>{formatNumber(chart?.statsSummary?.queryCountLast30Days)} queries</StatContent>,
        });
    }

    /**
     * Users column
     */
    if (chart?.statsSummary?.uniqueUserCountLast30Days) {
        columns.push({
            title: 'Users',
            content: <StatContent>{formatNumber(chart?.statsSummary?.uniqueUserCountLast30Days)} users</StatContent>,
        });
    }

    /**
     * Recent Views
     */
    if (chart?.statsSummary?.viewCountLast30Days) {
        columns.push({
            title: 'Recent Views',
            content: (
                <Tooltip
                    showArrow={false}
                    title={`${formatNumberWithoutAbbreviation(
                        chart?.statsSummary?.viewCountLast30Days,
                    )} views over the past 30 days`}
                >
                    <StatContent>{formatNumber(chart?.statsSummary?.viewCountLast30Days)} views</StatContent>
                </Tooltip>
            ),
        });
    }

    /**
     * Total Views
     */
    if (chart?.statsSummary?.viewCount) {
        columns.push({
            title: 'Total Views',
            content: (
                <Tooltip
                    showArrow={false}
                    title={`${formatNumberWithoutAbbreviation(chart?.statsSummary?.viewCount)} views`}
                >
                    <StatContent>{formatNumber(chart?.statsSummary?.viewCount)} views</StatContent>
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
