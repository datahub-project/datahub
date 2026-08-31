import { Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SidebarHeaderSectionColumns } from '@app/entityV2/shared/containers/profile/sidebar/SidebarHeaderSectionColumns';
import SidebarTopUsersHeaderSection from '@app/entityV2/shared/containers/profile/sidebar/shared/SidebarTopUsersHeaderSection';
import {
    getDatasetPopularityTier,
    isValuePresent,
    userExists,
} from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { SidebarStatsColumn, getPopularityColumn } from '@app/entityV2/shared/containers/profile/utils';
import CompactContext from '@app/shared/CompactContext';
import { formatBytes, formatNumber, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

const StatContent = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    font-weight: 600;
`;

const SidebarDatasetHeaderSection = () => {
    const { t } = useTranslation('entity.shared.containers');
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
        t,
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
            title: t('sidebar.datasetHeader.topUsersColumn'),
            content: (
                <Tooltip showArrow={false} title={t('sidebar.datasetHeader.topUsersTooltip')}>
                    <SidebarTopUsersHeaderSection />
                </Tooltip>
            ),
        });
    }

    /**
     * Queries column
     */
    if (dataset?.statsSummary?.queryCountLast30Days) {
        const count = dataset?.statsSummary?.queryCountLast30Days;
        columns.push({
            title: t('sidebar.datasetHeader.queriesColumn'),
            content: (
                <Tooltip
                    showArrow={false}
                    title={t('sidebar.datasetHeader.queriesOverPast30DaysTooltip', {
                        count,
                        formattedCount: formatNumberWithoutAbbreviation(count),
                    })}
                >
                    <StatContent>
                        {t('sidebar.datasetHeader.queriesCount', { count, formattedCount: formatNumber(count) })}
                    </StatContent>
                </Tooltip>
            ),
        });
    }

    /**
     * Users column
     */
    if (dataset?.statsSummary?.uniqueUserCountLast30Days) {
        const count = dataset?.statsSummary?.uniqueUserCountLast30Days;
        columns.push({
            title: t('sidebar.datasetHeader.usersColumn'),
            content: (
                <Tooltip
                    showArrow={false}
                    title={t('sidebar.datasetHeader.usersOverPast30DaysTooltip', {
                        count,
                        formattedCount: formatNumberWithoutAbbreviation(count),
                    })}
                >
                    <StatContent>
                        {t('sidebar.datasetHeader.usersCount', { count, formattedCount: formatNumber(count) })}
                    </StatContent>
                </Tooltip>
            ),
        });
    }

    /**
     * Rows column
     */
    if (isValuePresent(maybeLastProfile?.rowCount)) {
        const count = maybeLastProfile?.rowCount;
        columns.push({
            title: t('sidebar.datasetHeader.rowsColumn'),
            content: (
                <Tooltip
                    showArrow={false}
                    title={t('sidebar.datasetHeader.rowsTooltip', {
                        count,
                        formattedCount: formatNumberWithoutAbbreviation(count),
                    })}
                >
                    <StatContent>
                        {t('sidebar.datasetHeader.rowsCount', { count, formattedCount: formatNumber(count) })}
                    </StatContent>
                </Tooltip>
            ),
        });
    }

    /**
     * Column column
     */
    if (isValuePresent(maybeLastProfile?.columnCount)) {
        const count = maybeLastProfile?.columnCount;
        columns.push({
            title: t('sidebar.datasetHeader.columnsColumn'),
            content: (
                <StatContent>
                    {t('sidebar.datasetHeader.columnsCount', { count, formattedCount: formatNumber(count) })}
                </StatContent>
            ),
        });
    }

    /**
     * Size column
     */
    if (isValuePresent(maybeLastProfile?.sizeInBytes)) {
        const count = maybeLastProfile?.sizeInBytes;
        const formattedBytes = formatBytes(count, 0);
        const { number, unit } = formattedBytes;
        columns.push({
            title: t('sidebar.datasetHeader.sizeColumn'),
            content: (
                <Tooltip
                    showArrow={false}
                    title={t('sidebar.datasetHeader.storageSizeTooltip', {
                        count,
                        formattedCount: formatNumberWithoutAbbreviation(count),
                    })}
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
