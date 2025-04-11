import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '../../../../../../../entity/shared/EntityContext';
import CompactContext from '../../../../../../../shared/CompactContext';
import { formatBytes, formatNumber, formatNumberWithoutAbbreviation } from '../../../../../../../shared/formatNumber';
import { REDESIGN_COLORS } from '../../../../../constants';
import { getPopularityColumn, SidebarStatsColumn } from '../../../utils';
import SidebarTopUsersHeaderSection from '../../shared/SidebarTopUsersHeaderSection';
import { getDatasetPopularityTier, isValuePresent, userExists } from '../../shared/utils';
import { SidebarHeaderSectionColumns } from '../../SidebarHeaderSectionColumns';

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

    const latestFullTableProfile = dataset?.latestFullTableProfile?.[0];
    const latestPartitionProfile = dataset?.latestPartitionProfile?.[0];

    const maybeLastProfile = latestFullTableProfile || latestPartitionProfile || undefined;

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
            content: (
                <Tooltip showArrow={false} title="Top users over the past 30 days">
                    <SidebarTopUsersHeaderSection />
                </Tooltip>
            ),
        });
    }

    /**
     * Queries column
     */
    if (dataset?.statsSummary?.queryCountLast30Days) {
        columns.push({
            title: 'Queries',
            content: (
                <Tooltip
                    showArrow={false}
                    title={`${formatNumberWithoutAbbreviation(
                        dataset?.statsSummary?.queryCountLast30Days,
                    )} queries over the past 30 days`}
                >
                    <StatContent>{formatNumber(dataset?.statsSummary?.queryCountLast30Days)} queries</StatContent>
                </Tooltip>
            ),
        });
    }

    /**
     * Users column
     */
    if (dataset?.statsSummary?.uniqueUserCountLast30Days) {
        columns.push({
            title: 'Users',
            content: (
                <Tooltip
                    showArrow={false}
                    title={`${formatNumberWithoutAbbreviation(
                        dataset?.statsSummary?.uniqueUserCountLast30Days,
                    )} users over the past 30 days`}
                >
                    <StatContent>{formatNumber(dataset?.statsSummary?.uniqueUserCountLast30Days)} users</StatContent>
                </Tooltip>
            ),
        });
    }

    /**
     * Rows column
     */
    if (isValuePresent(maybeLastProfile?.rowCount)) {
        columns.push({
            title: 'Rows',
            content: (
                <Tooltip
                    showArrow={false}
                    title={`${formatNumberWithoutAbbreviation(maybeLastProfile?.rowCount)} rows`}
                >
                    <StatContent>{formatNumber(maybeLastProfile?.rowCount)} rows</StatContent>
                </Tooltip>
            ),
        });
    }

    /**
     * Column column
     */
    if (isValuePresent(maybeLastProfile?.columnCount)) {
        columns.push({
            title: 'Columns',
            content: <StatContent>{formatNumber(maybeLastProfile?.columnCount)} columns</StatContent>,
        });
    }

    /**
     * Size column
     */
    if (isValuePresent(maybeLastProfile?.sizeInBytes)) {
        const formattedBytes = formatBytes(maybeLastProfile?.sizeInBytes, 0);
        const { number, unit } = formattedBytes;
        columns.push({
            title: 'Size',
            content: (
                <Tooltip
                    showArrow={false}
                    title={`Consumes ${formatNumberWithoutAbbreviation(
                        maybeLastProfile?.sizeInBytes,
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
