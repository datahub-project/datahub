import React from 'react';
import styled from 'styled-components';
import { useEntityData } from '../../../../../../../entity/shared/EntityContext';
import { SidebarHeaderSectionColumns } from '../../SidebarHeaderSectionColumns';
import SidebarPopularityHeaderSection from '../../shared/SidebarPopularityHeaderSection';
import SidebarTopUsersHeaderSection from '../../shared/SidebarTopUsersHeaderSection';
import { isValuePresent, userExists } from '../../shared/utils';
import CompactContext from '../../../../../../../shared/CompactContext';
import { REDESIGN_COLORS } from '../../../../../constants';
import { formatNumber } from '../../../../../../../shared/formatNumber';
import { getDatasetLastUpdatedMs } from '../../../../../utils';
import Freshness, { getFreshnessTitle } from '../../../../../../../previewV2/Freshness';

const StatContent = styled.div`
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_4};
    font-size: 12px;
    font-weight: 600;
`;

const SidebarDatasetHeaderSection = () => {
    const { entityData } = useEntityData();
    const dataset = entityData as any;

    const isCompact = React.useContext(CompactContext);

    const columns: any = [];

    const lastOp = dataset?.lastOperation
        || dataset?.operations?.length && dataset?.operations[0]?.lastUpdatedTimestamp;
    const timeData = getDatasetLastUpdatedMs(dataset?.properties, lastOp);

    /**
     * Popularity tab
     */
    if (
        isValuePresent(dataset?.statsSummary?.queryCountPercentileLast30Days) &&
        isValuePresent(dataset?.statsSummary?.uniqueUserPercentileLast30Days)
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
        dataset?.statsSummary?.topUsersLast30Days &&
        dataset?.statsSummary?.topUsersLast30Days?.find((user) => userExists(user))
    ) {
        columns.push({
            title: 'Top Users',
            content: <SidebarTopUsersHeaderSection />,
        });
    }

    /**
     * Queries column
     */
    if (dataset?.statsSummary?.queryCountLast30Days) {
        columns.push({
            title: 'Queries',
            content: <StatContent>{formatNumber(dataset?.statsSummary?.queryCountLast30Days)} queries</StatContent>,
        });
    }

    /**
     * Users column
     */
    if (dataset?.statsSummary?.uniqueUserCountLast30Days) {
        columns.push({
            title: 'Users',
            content: <StatContent>{formatNumber(dataset?.statsSummary?.uniqueUserCountLast30Days)} users</StatContent>,
        });
    }

    /**
     * Freshness column
     */
    if (timeData?.lastUpdatedMs) {
        columns.push({
            title: getFreshnessTitle(timeData?.property),
            content: <Freshness time={timeData.lastUpdatedMs} timeProperty={timeData.property} />,
        });
    }

    if (!columns.length && !isCompact) {
        return null;
    }

    return <SidebarHeaderSectionColumns columns={columns} />;
};

export default SidebarDatasetHeaderSection;
