import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { Divider, Typography } from 'antd';
import ProfilingRunsChart from './charts/ProfilingRunsChart';
import StatChart from './charts/StatChart';
import { DatasetProfile, DateInterval } from '../../../../../../../types.generated';
import { getFixedLookbackWindow, TimeWindowSize } from '../../../../../../shared/time/timeUtils';
import { useGetDataProfilesLazyQuery } from '../../../../../../../graphql/dataset.generated';
import { Message } from '../../../../../../shared/Message';
import { LookbackWindow } from '../lookbackWindows';
import { ANTD_GRAY } from '../../../../constants';
import PrefixedSelect from './shared/PrefixedSelect';
import { formatBytes } from '../../../../../../shared/formatNumber';

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

const isPresent = (val: any) => {
    return val !== undefined && val !== null;
};

/**
 * Extracts a set of points used to render charts from a list of Dataset Profiles +
 * a particular numeric statistic name to extract. Note that the stat *must* be numeric for this utility to work.
 */
const extractChartValuesFromTableProfiles = (profiles: Array<any>, statName: string) => {
    return profiles
        .filter((profile) => isPresent(profile[statName]))
        .map((profile) => ({
            timeMs: profile.timestampMillis,
            value: profile[statName] as number,
        }));
};

/**
 * Extracts a set of field-specific points used to render charts from a list of Dataset Profiles +
 * a particular numeric statistic name to extract. Note that the stat *must* be numeric for this utility to work.
 */
const extractChartValuesFromFieldProfiles = (profiles: Array<any>, fieldPath: string, statName: string) => {
    return profiles
        .filter((profile) => profile.fieldProfiles)
        .map((profile) => {
            const fieldProfiles = profile.fieldProfiles
                ?.filter((field) => field.fieldPath === fieldPath)
                .filter((field) => field[statName] !== null && field[statName] !== undefined);

            if (fieldProfiles?.length === 1) {
                const fieldProfile = fieldProfiles[0];
                return {
                    timeMs: profile.timestampMillis,
                    value: fieldProfile[statName],
                };
            }
            return null;
        })
        .filter((value) => value !== null);
};

const computeChartTickInterval = (windowSize: TimeWindowSize): DateInterval => {
    switch (windowSize.interval) {
        case DateInterval.Day:
            return DateInterval.Hour;
        case DateInterval.Week:
            return DateInterval.Day;
        case DateInterval.Month:
            return DateInterval.Week;
        case DateInterval.Year:
            return DateInterval.Month;
        default:
            throw new Error(`Unrecognized DateInterval provided ${windowSize.interval}`);
    }
};

const computeAllFieldPaths = (profiles: Array<DatasetProfile>): Set<string> => {
    const uniqueFieldPaths = new Set<string>();
    profiles.forEach((profile) => {
        const fieldProfiles = profile.fieldProfiles || [];
        fieldProfiles.forEach((fieldProfile) => {
            uniqueFieldPaths.add(fieldProfile.fieldPath);
        });
    });
    return uniqueFieldPaths;
};

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

    return (
        <>
            {profilesLoading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            <StatSection>
                <Typography.Title level={5}>Profiling Runs</Typography.Title>
                <ProfilingRunsChart profiles={profiles} />
            </StatSection>
            <StatSection>
                <Typography.Title level={5}>Table Stats</Typography.Title>
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
