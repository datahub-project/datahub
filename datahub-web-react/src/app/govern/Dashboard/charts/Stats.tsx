import React, { useEffect } from 'react';

import { sortBy } from 'lodash';

import { SimpleLineChart } from '../../../dataviz/line/SimpleLineChart';
import { ChartCard } from '../../../dataviz';

import { mergeRowAndHeaderData, formatPercentage } from '../utils';
import { useFormAnalyticsQuery } from '../../../../graphql/analytics.generated';
import { useFormAnalyticsContext } from '../FormAnalyticsContext';

import { ChartGroup, Row, StatusSeriesWrapper, StatusSeriesHeading, StatusSeriesDescription } from '../components';

import { ChartNoData, ChartNotEnoughData, ChartState } from './AuxViews';

const getTabType = (selectedTab) => {
    let dataType = '';
    if (selectedTab === 'byForm') dataType = 'assigned to this form';
    if (selectedTab === 'byAssignee') dataType = 'assigned to this user or group';
    if (selectedTab === 'byDomain') dataType = 'in this domain assigned';
    return dataType;
};

const getDescription = (assetCount, totalAssetCount, selectedTab, series) =>
    `${assetCount} of ${totalAssetCount.toLocaleString()} assets ${getTabType(selectedTab)} ${
        series.key !== 10000 ? `in the ${series.label.toLowerCase()}` : ''
    }`;

// March/2024 launch decision: hide trendline but keep code
const hideTrendLine = true;

const CompletedTrend = () => {
    const {
        sql,
        timeSeries: { getSeriesInfo },
        sectionLoadStates: { stats, setLoadStates },
        tabs: { selectedTab },
    } = useFormAnalyticsContext();

    const {
        data: percentCount,
        loading: percentCountLoading,
        error: percentCountError,
    } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.completedTrendPercentAndCount } },
        skip: sql.skip,
    });

    const {
        data: trend,
        loading: trendLoading,
        error: trendError,
    } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.completedTrend } },
        skip: true, // TODO: not hardcode this once we start showing trend lines - this query is expensive (and we're not using it yet)
    });

    useEffect(() => {
        if (!percentCountLoading && !trendLoading && !stats) setLoadStates('stats', 'completedTrend', true);
    }, [percentCountLoading, trendLoading, setLoadStates, stats]);

    // States the chart can be in
    const chartState = {
        loading: percentCountLoading && trendLoading,
        error: !!percentCountError || !!trendError,
        noDataTimeframe: percentCount?.formAnalytics?.table?.length === 0,
        noData: percentCount?.formAnalytics?.table?.length === 0,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const percentCountMerged = mergeRowAndHeaderData(
        percentCount?.formAnalytics?.header,
        percentCount?.formAnalytics?.table || [],
    );

    const trendMerged = mergeRowAndHeaderData(trend?.formAnalytics?.header, trend?.formAnalytics?.table || []);
    if (percentCountMerged.length === 0) return <ChartNoData />;

    return (
        <StatusSeriesWrapper>
            <StatusSeriesHeading>
                {formatPercentage(percentCountMerged[0].completed_asset_percent || 0)} Completed
            </StatusSeriesHeading>
            <StatusSeriesDescription>
                {getDescription(
                    percentCountMerged[0].completed_asset_count,
                    percentCountMerged[0].assigned_asset_count,
                    selectedTab,
                    getSeriesInfo(),
                )}
            </StatusSeriesDescription>
            {!hideTrendLine &&
                (trendMerged.length <= 1 ? (
                    <ChartNotEnoughData />
                ) : (
                    <SimpleLineChart data={sortBy(trendMerged, ['date'])} />
                ))}
        </StatusSeriesWrapper>
    );
};

const InProgressTrend = () => {
    const {
        sql,
        timeSeries: { getSeriesInfo },
        sectionLoadStates: { stats, setLoadStates },
        tabs: { selectedTab },
    } = useFormAnalyticsContext();

    const {
        data: percentCount,
        loading: percentCountLoading,
        error: percentCountError,
    } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.inProgressTrendPercentAndCount } },
        skip: sql.skip,
    });

    const {
        data: trend,
        loading: trendLoading,
        error: trendError,
    } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.inProgressTrend } },
        skip: true, // TODO: not hardcode this once we start showing trend lines - this query is expensive (and we're not using it yet)
    });

    useEffect(() => {
        if (!percentCountLoading && !trendLoading && !stats) setLoadStates('stats', 'inProgressTrend', true);
    }, [percentCountLoading, trendLoading, setLoadStates, stats]);

    // States the chart can be in
    const chartState = {
        loading: percentCountLoading && trendLoading,
        error: !!percentCountError || !!trendError,
        noDataTimeframe: percentCount?.formAnalytics?.table?.length === 0,
        noData: percentCount?.formAnalytics?.table?.length === 0,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const percentCountMerged = mergeRowAndHeaderData(
        percentCount?.formAnalytics?.header,
        percentCount?.formAnalytics?.table || [],
    );

    const trendMerged = mergeRowAndHeaderData(trend?.formAnalytics?.header, trend?.formAnalytics?.table || []);
    if (percentCountMerged.length === 0) return <ChartNoData />;

    return (
        <StatusSeriesWrapper>
            <StatusSeriesHeading>
                {formatPercentage(percentCountMerged[0].completed_asset_percent || 0)} In Progress
            </StatusSeriesHeading>
            <StatusSeriesDescription>
                {getDescription(
                    percentCountMerged[0].completed_asset_count,
                    percentCountMerged[0].assigned_asset_count,
                    selectedTab,
                    getSeriesInfo(),
                )}
            </StatusSeriesDescription>
            {!hideTrendLine &&
                (trendMerged.length <= 1 ? (
                    <ChartNotEnoughData />
                ) : (
                    <SimpleLineChart data={sortBy(trendMerged, ['date'])} />
                ))}
        </StatusSeriesWrapper>
    );
};

const NotStartedTrend = () => {
    const {
        sql,
        timeSeries: { getSeriesInfo },
        sectionLoadStates: { stats, setLoadStates },
        tabs: { selectedTab },
    } = useFormAnalyticsContext();

    const {
        data: percentCount,
        loading: percentCountLoading,
        error: percentCountError,
    } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.notStartedTrendPercentAndCount } },
        skip: sql.skip,
    });

    const {
        data: trend,
        loading: trendLoading,
        error: trendError,
    } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.notStartedTrend } },
        skip: true, // TODO: not hardcode this once we start showing trend lines - this query is expensive (and we're not using it yet)
    });

    useEffect(() => {
        if (!percentCountLoading && !trendLoading && !stats) setLoadStates('stats', 'notStartedTrend', true);
    }, [percentCountLoading, trendLoading, setLoadStates, stats]);

    // States the chart can be in
    const chartState = {
        loading: percentCountLoading && trendLoading,
        error: !!percentCountError || !!trendError,
        noDataTimeframe: percentCount?.formAnalytics?.table?.length === 0,
        noData: percentCount?.formAnalytics?.table?.length === 0,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const percentCountMerged = mergeRowAndHeaderData(
        percentCount?.formAnalytics?.header,
        percentCount?.formAnalytics?.table || [],
    );

    const trendMerged = mergeRowAndHeaderData(trend?.formAnalytics?.header, trend?.formAnalytics?.table || []);
    if (percentCountMerged.length === 0) return <ChartNoData />;

    return (
        <StatusSeriesWrapper>
            <StatusSeriesHeading>
                {formatPercentage(percentCountMerged[0].completed_asset_percent || 0)} Not Started
            </StatusSeriesHeading>
            <StatusSeriesDescription>
                {getDescription(
                    percentCountMerged[0].completed_asset_count,
                    percentCountMerged[0].assigned_asset_count,
                    selectedTab,
                    getSeriesInfo(),
                )}
            </StatusSeriesDescription>
            {!hideTrendLine &&
                (trendMerged.length <= 1 ? (
                    <ChartNotEnoughData />
                ) : (
                    <SimpleLineChart data={sortBy(trendMerged, ['date'])} />
                ))}
        </StatusSeriesWrapper>
    );
};

// this section is always first, so no condtional render for waterfall load
export const Stats = () => (
    <ChartGroup>
        <Row>
            <ChartCard
                title="Assets Without Completed Documentation"
                titleInfo='Assets are "Not Started" if they have no forms in progress or complete'
                chart={<NotStartedTrend />}
            />
            <ChartCard
                title="Assets With Documention In Progress"
                titleInfo='Assets are "In Progress" if they&apos;ve started on any forms and have not completed all of them'
                chart={<InProgressTrend />}
            />
            <ChartCard
                title="Assets With Completed Documentation"
                titleInfo='Assets are "Completed" if all of their forms are completed'
                chart={<CompletedTrend />}
            />
        </Row>
    </ChartGroup>
);
