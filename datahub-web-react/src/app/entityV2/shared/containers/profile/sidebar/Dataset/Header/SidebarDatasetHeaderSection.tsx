import React from 'react';
import styled from 'styled-components';
import { Tooltip } from 'antd';

import { useEntityData } from '../../../../../../../entity/shared/EntityContext';
import { getPopularityColumn, SidebarStatsColumn } from '../../../utils';
import { SidebarHeaderSectionColumns } from '../../SidebarHeaderSectionColumns';
import SidebarTopUsersHeaderSection from '../../shared/SidebarTopUsersHeaderSection';
import { getDatasetPopularityTier, isValuePresent, userExists } from '../../shared/utils';
import CompactContext from '../../../../../../../shared/CompactContext';
import { REDESIGN_COLORS } from '../../../../../constants';
import { formatBytes, formatNumber, formatNumberWithoutAbbreviation } from '../../../../../../../shared/formatNumber';

const StatContent = styled.div`
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_4};
    font-size: 12px;
    font-weight: 600;
`;

const SidebarDatasetHeaderSection = () => {
    const { entityData } = useEntityData();
    const dataset = entityData as any;

    const isCompact = React.useContext(CompactContext);

    const columns: SidebarStatsColumn[] = [];

    /**
     * Popularity tab
     */
    const popularityColumn = getPopularityColumn(
        getDatasetPopularityTier(
            dataset?.statsSummary?.queryCountPercentileLast30Days,
            dataset?.statsSummary?.uniqueUserPercentileLast30Days,
        ),
    );
    if (popularityColumn) {
        columns.push(popularityColumn);
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
     * Rows column
     */
    if (isValuePresent(dataset?.datasetProfiles?.[0]?.rowCount)) {
        columns.push({
            title: 'Rows',
            content: <StatContent>{formatNumber(dataset?.datasetProfiles[0]?.rowCount)} rows</StatContent>,
        });
    }

    /**
     * Column column
     */
    if (isValuePresent(dataset?.datasetProfiles?.[0]?.columnCount)) {
        columns.push({
            title: 'Columns',
            content: <StatContent>{formatNumber(dataset?.datasetProfiles[0]?.columnCount)} columns</StatContent>,
        });
    }

    /**
     * Size column
     */
    if (isValuePresent(dataset?.datasetProfiles?.[0]?.sizeInBytes)) {
        const formattedBytes = formatBytes(dataset?.datasetProfiles[0]?.sizeInBytes, 0);
        const { number, unit } = formattedBytes;
        columns.push({
            title: 'Size',
            content: (
                <Tooltip
                    title={`This asset consumes ${formatNumberWithoutAbbreviation(
                        dataset?.datasetProfiles[0]?.sizeInBytes,
                    )} bytes of storage.`}
                >
                    <StatContent>
                        {number} {unit}
                    </StatContent>
                </Tooltip>
            ),
        });
    }

    if (!columns.length && !isCompact) {
        return null;
    }

    return <SidebarHeaderSectionColumns columns={columns} />;
};

export default SidebarDatasetHeaderSection;
