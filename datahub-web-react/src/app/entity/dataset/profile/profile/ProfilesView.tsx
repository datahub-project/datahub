import React, { useEffect, useState } from 'react';
import { Affix, Divider, Radio, Row, Select, Space, Typography } from 'antd';
import { useGetDataProfilesLazyQuery } from '../../../../../graphql/dataset.generated';
import { DatasetProfile, DateInterval, TimeRange } from '../../../../../types.generated';
import ProfilingRunsChart from './ProfilingRunsChart';
import RowCountChart from './RowCountChart';
import ColumnCountChart from './ColumnCountChart';
import DistinctCountChart from './DistinctCountChart';
import DistinctPercentageChart from './DistinctPercentageChart';
import NullCountChart from './NullCountChart';
import NullPercentageChart from './NullPercentageChart';
import { Message } from '../../../../shared/Message';
import DataProfileView from './DataProfileView';

export type Props = {
    urn: string;
    profile?: DatasetProfile;
};

const getTimeRangeMillis = (timeRange: TimeRange) => {
    const oneHourMillis = 60 * 60 * 1000;
    const oneDayMillis = 24 * oneHourMillis;
    switch (timeRange) {
        case TimeRange.Day:
            return 2 * oneDayMillis + 1;
        case TimeRange.Week:
            return 8 * oneDayMillis + 1;
        case TimeRange.Month:
            return 31 * oneDayMillis + 1;
        case TimeRange.Quarter:
            return 92 * oneDayMillis + 1;
        case TimeRange.Year:
            return 366 * oneDayMillis + 1;
        default:
            throw new Error(`Unrecognized TimeRange provided ${timeRange}`);
    }
};

const computeNumericTimeRange = (end: number, timeRange: TimeRange): { start: string; end: string } => {
    return {
        start: `${end - getTimeRangeMillis(timeRange)}`,
        end: `${end}`,
    };
};

const computeDateInterval = (timeRange: TimeRange): DateInterval => {
    switch (timeRange) {
        case TimeRange.Day:
            return DateInterval.Hour;
        case TimeRange.Week:
            return DateInterval.Day;
        case TimeRange.Month:
            return DateInterval.Week;
        case TimeRange.Quarter:
            return DateInterval.Week;
        case TimeRange.Year:
            return DateInterval.Month;
        default:
            throw new Error(`Unrecognized TimeRange provided ${timeRange}`);
    }
};

const computeAllFieldPaths = (profiles: Array<DatasetProfile>): Set<string> => {
    const uniqueFieldPaths = new Set<string>();
    profiles.forEach((profile) => {
        if (profile.fieldProfiles) {
            profile.fieldProfiles.forEach((fieldProfile) => {
                uniqueFieldPaths.add(fieldProfile.fieldPath);
            });
        }
    });
    return uniqueFieldPaths;
};

const DEFAULT_TIME_RANGE = TimeRange.Week;

export default function ProfilesView({ urn, profile }: Props) {
    const [getDataProfiles, { data: historicalProfileData, loading: historicalProfileLoading }] =
        useGetDataProfilesLazyQuery();

    const [historicalTimeRange, setHistoricalTimeRange] = useState(DEFAULT_TIME_RANGE);

    const handleHistoricalTimeRangeChange = (value) => {
        getDataProfiles({
            variables: { urn, range: value },
        });
        setHistoricalTimeRange(value);
    };

    useEffect(() => {
        getDataProfiles({
            variables: { urn, range: DEFAULT_TIME_RANGE },
        });
    }, [urn, getDataProfiles]);

    const [selectedFieldPath, setSelectedFieldPath] = useState('');

    const handleSelectedFieldPathChange = (value) => {
        setSelectedFieldPath(value);
    };

    const [view, setView] = useState('latest');

    const handleViewChange = (e) => {
        setView(e.target.value);
    };

    if (!profile) {
        return <Typography.Text>No Profiles Found</Typography.Text>;
    }

    const lastUpdatedDate = new Date(profile.timestampMillis);

    const timeRanges = [
        { text: '1 day', timeRange: TimeRange.Day },
        { text: '1 week', timeRange: TimeRange.Week },
        { text: '1 month', timeRange: TimeRange.Month },
        { text: '3 months', timeRange: TimeRange.Quarter },
        { text: '1 year', timeRange: TimeRange.Year },
    ];

    // Todo: put in a time utility.
    const dateInterval = computeDateInterval(historicalTimeRange);
    const dateRange = computeNumericTimeRange(Date.now(), historicalTimeRange);

    const allFieldPaths = historicalProfileData?.dataset?.profiles
        ? Array.from(computeAllFieldPaths(historicalProfileData?.dataset?.profiles as any))
        : new Array<string>();

    if (!selectedFieldPath && allFieldPaths.length > 0) {
        setSelectedFieldPath(allFieldPaths[0]);
    }

    // TODO: Revisit the layout here.
    return (
        <>
            <Space direction="vertical" style={{ marginTop: 20, width: '100%' }} size={40}>
                <Affix offsetTop={127}>
                    <Row
                        style={{ paddingTop: 12, paddingBottom: 16, backgroundColor: 'white' }}
                        justify="space-between"
                        align="middle"
                    >
                        {view === 'latest' && (
                            <Space direction="vertical" size={0}>
                                <Typography.Title level={2}>Latest Stats</Typography.Title>
                                <Typography.Text style={{ color: 'gray' }}>
                                    Reported at {lastUpdatedDate.toLocaleDateString()} at{' '}
                                    {lastUpdatedDate.toLocaleTimeString()}
                                </Typography.Text>
                            </Space>
                        )}
                        {view === 'historical' && (
                            <Space direction="vertical" size={0}>
                                <Typography.Title level={2}>Profiling History</Typography.Title>
                                <span>
                                    <Typography.Text style={{ color: 'grey', fontSize: 16 }}>
                                        Viewing profiling history for the past
                                    </Typography.Text>
                                    <Select
                                        style={{ width: 120, paddingLeft: 8 }}
                                        value={historicalTimeRange}
                                        onChange={handleHistoricalTimeRangeChange}
                                    >
                                        {timeRanges.map((timeRange) => (
                                            <Select.Option value={timeRange.timeRange}>{timeRange.text}</Select.Option>
                                        ))}
                                    </Select>
                                </span>
                            </Space>
                        )}
                        <Radio.Group value={view} onChange={handleViewChange}>
                            <Radio.Button value="latest">Latest</Radio.Button>
                            <Radio.Button value="historical">Historical</Radio.Button>
                        </Radio.Group>
                    </Row>
                </Affix>
                {view === 'latest' && <DataProfileView profile={profile} />}
                {view === 'historical' && (
                    <Space direction="vertical" size={60}>
                        {historicalProfileLoading && (
                            <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />
                        )}
                        <Space direction="vertical" style={{ width: '100%' }}>
                            <Typography.Title level={3}>Profiling Runs</Typography.Title>
                            <Divider style={{ margin: 0 }} />
                            <ProfilingRunsChart profiles={(historicalProfileData?.dataset?.profiles as any) || []} />
                        </Space>
                        <Space direction="vertical" style={{ width: '100%' }}>
                            <Typography.Title level={3}>Historical Table Stats</Typography.Title>
                            <Divider style={{ margin: 0 }} />
                            <Row>
                                <RowCountChart
                                    interval={dateInterval}
                                    dateRange={dateRange}
                                    profiles={(historicalProfileData?.dataset?.profiles as any) || []}
                                />
                                <ColumnCountChart
                                    interval={dateInterval}
                                    dateRange={dateRange}
                                    profiles={(historicalProfileData?.dataset?.profiles as any) || []}
                                />
                            </Row>
                        </Space>
                        <Space direction="vertical" style={{ marginBottom: 80 }}>
                            <Row justify="space-between">
                                <Typography.Title level={3}>Historical Column Stats</Typography.Title>
                                <span>
                                    <Typography.Text style={{ fontSize: 16, color: 'grey' }}>
                                        Viewing stats for column
                                    </Typography.Text>
                                    <Select
                                        style={{ marginLeft: 8, width: 160 }}
                                        value={selectedFieldPath}
                                        onChange={handleSelectedFieldPathChange}
                                    >
                                        {allFieldPaths.map((fieldPath) => (
                                            <Select.Option value={fieldPath}>{fieldPath}</Select.Option>
                                        ))}
                                    </Select>
                                </span>
                            </Row>
                            <Divider style={{ margin: 0 }} />
                            <Row>
                                <NullCountChart
                                    fieldPath={selectedFieldPath}
                                    interval={dateInterval}
                                    dateRange={dateRange}
                                    profiles={(historicalProfileData?.dataset?.profiles as any) || []}
                                />
                                <NullPercentageChart
                                    fieldPath={selectedFieldPath}
                                    interval={dateInterval}
                                    dateRange={dateRange}
                                    profiles={(historicalProfileData?.dataset?.profiles as any) || []}
                                />
                                <DistinctCountChart
                                    interval={dateInterval}
                                    fieldPath={selectedFieldPath}
                                    dateRange={dateRange}
                                    profiles={(historicalProfileData?.dataset?.profiles as any) || []}
                                />
                                <DistinctPercentageChart
                                    fieldPath={selectedFieldPath}
                                    interval={dateInterval}
                                    dateRange={dateRange}
                                    profiles={(historicalProfileData?.dataset?.profiles as any) || []}
                                />
                            </Row>
                        </Space>
                    </Space>
                )}
            </Space>
        </>
    );
}
