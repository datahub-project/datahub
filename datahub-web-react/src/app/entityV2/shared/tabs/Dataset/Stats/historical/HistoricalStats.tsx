import { Divider, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useGetDataProfilesLazyQuery } from '../../../../../../../graphql/dataset.generated';
import { Message } from '../../../../../../shared/Message';
import { formatBytes } from '../../../../../../shared/formatNumber';
import { getFixedLookbackWindow } from '../../../../../../shared/time/timeUtils';
import { ANTD_GRAY } from '../../../../constants';
import {
    computeAllFieldPaths,
    computeChartTickInterval,
    extractChartValuesFromFieldProfiles,
    extractChartValuesFromTableProfiles,
} from '../../../../utils';
import { FULL_TABLE_PARTITION_KEYS } from '../constants';
import { LookbackWindow } from '../lookbackWindows';
import ProfilingRunsChart from './charts/ProfilingRunsChart';
import StatChart from './charts/StatChart';
import PrefixedSelect from './shared/PrefixedSelect';

// TODO: Reuse stat sections.
const StatSection = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    padding: 16px 20px;
    margin-top: 12px;
`;

const ColumnStatsHeader = styled.div`
    display: flex;
    justify-content: space-between;
`;

const ChartRow = styled.div`
    display: flex;
    justify-content: space-around;
    padding: 24px;
`;

const ChartDivider = styled(Divider)<{ height: number; width: number }>`
    background-color: ${ANTD_GRAY[0]};
    height: ${(props) => props.height}px;
    width: ${(props) => props.width}px;
    margin: 20px;
`;

const getLookbackWindowSize = (window: LookbackWindow) => {
    return window.windowSize;
};

export type Props = {
    urn: string;
    lookbackWindow: LookbackWindow;
};

export default function HistoricalStats({ urn, lookbackWindow }: Props) {
    const [getDataProfiles, { data: profilesData, loading: profilesLoading }] = useGetDataProfilesLazyQuery();

    /**
     * Perform initial fetch of default lookback window stats.
     */
    useEffect(() => {
        getDataProfiles({
            variables: { urn, ...getFixedLookbackWindow(getLookbackWindowSize(lookbackWindow)) },
        });
    }, [urn, lookbackWindow, getDataProfiles]);

    /**
     * Determines which fixed lookback window is used to display historical statistics. See above for valid options.
     */
    const selectedWindowSize = getLookbackWindowSize(lookbackWindow);
    const selectedWindow = getFixedLookbackWindow(selectedWindowSize);

    /**
     * Determines which field path is highlighted in column stats. Defaults to none.
     */
    const [selectedFieldPath, setSelectedFieldPath] = useState('');

    const onChangeSelectedFieldPath = (value) => {
        setSelectedFieldPath(value);
    };

    const graphTickInterval = computeChartTickInterval(selectedWindowSize);
    const graphDateRange = {
        start: selectedWindow.startTime.toString(),
        end: selectedWindow.endTime.toString(),
    };

    const profiles = profilesData?.dataset?.datasetProfiles || [];
    const allFieldPaths = Array.from(computeAllFieldPaths(profiles));

    if (selectedFieldPath === '' && allFieldPaths.length > 0) {
        // Set initially selected field path.
        setSelectedFieldPath(allFieldPaths[0]);
    }

    const columnSelectView = (
        <PrefixedSelect
            prefixText="Stats for column "
            values={allFieldPaths}
            value={selectedFieldPath}
            setValue={onChangeSelectedFieldPath}
        />
    );

    /**
     * Compute Table Stat chart data.
     */
    const rowCountChartValues = extractChartValuesFromTableProfiles(profiles, 'rowCount');
    const columnCountChartValues = extractChartValuesFromTableProfiles(profiles, 'columnCount');
    const sizeChartValues = extractChartValuesFromTableProfiles(profiles, 'sizeInBytes');

    /**
     * Compute Column Stat chart data.
     */
    const nullCountChartValues: Array<any> = extractChartValuesFromFieldProfiles(
        profiles,
        selectedFieldPath,
        'nullCount',
    );
    const nullPercentageChartValues: Array<any> = extractChartValuesFromFieldProfiles(
        profiles,
        selectedFieldPath,
        'nullProportion',
    );
    const distinctCountChartValues: Array<any> = extractChartValuesFromFieldProfiles(
        profiles,
        selectedFieldPath,
        'uniqueCount',
    );
    const distinctPercentageChartValues: Array<any> = extractChartValuesFromFieldProfiles(
        profiles,
        selectedFieldPath,
        'uniqueProportion',
    );

    const bytesFormatter = (num: number) => {
        const formattedBytes = formatBytes(num);
        return `${formattedBytes.number} ${formattedBytes.unit}`;
    };

    const placeholderChart = (
        <StatChart
            title="Placeholder"
            tickInterval={graphTickInterval}
            dateRange={{ start: '', end: '' }}
            values={[]}
            visible={false}
        />
    );
    const placeholderVerticalDivider = (
        <ChartDivider type="vertical" height={360} width={1} style={{ visibility: 'hidden' }} />
    );

    const areAllProfilesPartitioned = profiles.every(
        (profile) => !FULL_TABLE_PARTITION_KEYS.includes(profile.partitionSpec?.partition || ''),
    );

    return (
        <>
            {profilesLoading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            <StatSection>
                <Typography.Title level={5}>Profiling Runs</Typography.Title>
                <ProfilingRunsChart profiles={profiles} areAllProfilesPartitioned={areAllProfilesPartitioned} />
            </StatSection>
            <StatSection>
                <Typography.Title level={5}>
                    {areAllProfilesPartitioned ? 'Partition Stats' : 'Table Stats'}
                </Typography.Title>
                <ChartRow>
                    <StatChart
                        title="Row Count Over Time"
                        tickInterval={graphTickInterval}
                        dateRange={graphDateRange}
                        values={rowCountChartValues}
                    />
                    <ChartDivider type="vertical" height={360} width={1} />
                    <StatChart
                        title="Column Count Over Time"
                        tickInterval={graphTickInterval}
                        dateRange={graphDateRange}
                        values={columnCountChartValues}
                    />
                </ChartRow>
                <ChartDivider type="horizontal" height={1} width={400} />
                <ChartRow>
                    <StatChart
                        title="Size Over Time"
                        tickInterval={graphTickInterval}
                        dateRange={graphDateRange}
                        values={sizeChartValues}
                        yAxis={{ formatter: bytesFormatter }}
                    />
                    {placeholderVerticalDivider}
                    {placeholderChart}
                </ChartRow>
            </StatSection>
            <StatSection>
                <ColumnStatsHeader>
                    <Typography.Title level={5}>Column Stats</Typography.Title>
                    {columnSelectView}
                </ColumnStatsHeader>
                <ChartRow>
                    <StatChart
                        title="Null Count Over Time"
                        tickInterval={graphTickInterval}
                        dateRange={graphDateRange}
                        values={nullCountChartValues}
                    />
                    <ChartDivider type="vertical" height={360} width={1} />
                    <StatChart
                        title="Null Percentage Over Time"
                        tickInterval={graphTickInterval}
                        dateRange={graphDateRange}
                        values={nullPercentageChartValues}
                    />
                </ChartRow>
                <ChartDivider type="horizontal" height={1} width={400} />
                <ChartRow>
                    <StatChart
                        title="Distinct Count Over Time"
                        tickInterval={graphTickInterval}
                        dateRange={graphDateRange}
                        values={distinctCountChartValues}
                    />
                    <ChartDivider type="vertical" height={360} width={1} />
                    <StatChart
                        title="Distinct Percentage Over Time"
                        tickInterval={graphTickInterval}
                        dateRange={graphDateRange}
                        values={distinctPercentageChartValues}
                    />
                </ChartRow>
            </StatSection>
        </>
    );
}
