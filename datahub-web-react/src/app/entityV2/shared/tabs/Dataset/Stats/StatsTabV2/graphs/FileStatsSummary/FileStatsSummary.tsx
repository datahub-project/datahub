import { GraphCard, Text } from '@components';
import React, { useEffect, useMemo } from 'react';
import styled from 'styled-components';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import NoPermission from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/NoPermission';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { formatBytes, formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';

const StatList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const StatRow = styled.div`
    display: flex;
    justify-content: space-between;
    gap: 12px;
`;

const EMPTY_MESSAGE = 'No file statistics collected for this asset yet.';

const FileStatsSummary = () => {
    const {
        statsEntity,
        permissions: { canViewDatasetProfile },
        sections,
        setSectionState,
        dataInfo: { capabilitiesLoading },
    } = useStatsSectionsContext();

    const latestFullTableProfile = (statsEntity as any)?.latestFullTableProfile?.[0];
    const latestPartitionProfile = (statsEntity as any)?.latestPartitionProfile?.[0];
    const latestProfile = latestFullTableProfile || latestPartitionProfile;

    const metrics = useMemo(
        () => [
            { label: 'Total files', value: latestProfile?.totalFiles, formatter: formatNumberWithoutAbbreviation },
            { label: 'Manifest files', value: latestProfile?.totalManifestFiles, formatter: formatNumberWithoutAbbreviation },
            { label: 'Delete files', value: latestProfile?.totalDeleteFiles, formatter: formatNumberWithoutAbbreviation },
            { label: 'Files < 100MB', value: latestProfile?.totalFilesLessThan100Mb, formatter: formatNumberWithoutAbbreviation },
            { label: 'Total partitions', value: latestProfile?.totalPartitions, formatter: formatNumberWithoutAbbreviation },
            {
                label: 'Avg file size',
                value: latestProfile?.avgFileSizeBytes,
                formatter: (value: number) => {
                    const formatted = formatBytes(value, 2, 'B');
                    return `${formatNumberWithoutAbbreviation(formatted.number)} ${formatted.unit}`;
                },
            },
        ],
        [latestProfile],
    );

    const hasData = metrics.some((metric) => metric.value !== undefined && metric.value !== null);

    useEffect(() => {
        const currentSection = sections.fileStats;
        const isLoading = capabilitiesLoading;
        if (currentSection.hasData !== hasData || currentSection.isLoading !== isLoading) {
            setSectionState(SectionKeys.FILE_STATS, hasData, isLoading);
        }
    }, [capabilitiesLoading, hasData, sections.fileStats, setSectionState]);

    return (
        <GraphCard
            title="File Stats"
            isEmpty={!canViewDatasetProfile || !hasData}
            emptyContent={!canViewDatasetProfile ? <NoPermission statName="file stats" /> : EMPTY_MESSAGE}
            loading={capabilitiesLoading}
            graphHeight="fit-content"
            renderGraph={() => (
                <StatList>
                    {metrics.map((metric) => (
                        <StatRow key={metric.label}>
                            <Text size="sm" color="gray">
                                {metric.label}
                            </Text>
                            <Text size="sm">
                                {metric.value !== undefined && metric.value !== null
                                    ? metric.formatter(metric.value)
                                    : '—'}
                            </Text>
                        </StatRow>
                    ))}
                </StatList>
            )}
        />
    );
};

export default FileStatsSummary;
