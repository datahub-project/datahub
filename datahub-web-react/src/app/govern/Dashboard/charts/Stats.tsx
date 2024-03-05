import React, { useEffect } from 'react';

import { sortBy } from 'lodash';

import { SimpleLineChart } from '../../../dataviz/line/SimpleLineChart';
import { ChartCard } from '../../../dataviz';

import { mergeRowAndHeaderData, formatPercentage } from '../utils';
import { useFormAnalyticsQuery } from '../../../../graphql/analytics.generated';
import { useFormAnalyticsContext } from '../FormAnalyticsContext';

import {
	ChartGroup,
	Row,
	StatusSeriesWrapper,
	StatusSeriesHeading,
	StatusSeriesDescription
} from '../components';

import { ChartNoData, ChartState } from './AuxViews';

const CompletedTrend = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { stats, setLoadStates },
		tabs: { selectedTab },
		byAssignee: { selectedAssignee },
		byDomain: { selectedDomain }
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let percentCountQuery = sql.completedTrendPercentAndCount;
	if (selectedTab === 'byAssignee') percentCountQuery = sql.completedTrendPercentAndCountByAssignee;
	if (selectedTab === 'byDomain') percentCountQuery = sql.completedTrendPercentAndCountByDomain;

	const { data: percentCount, loading: percentCountLoading, error: percentCountError } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': percentCountQuery } },
		skip:
			!snapshot
			|| selectedTab === 'byAssignee' && !selectedAssignee
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	// Switch the query based on the selected tab
	let trendQuery = sql.completedTrend;
	if (selectedTab === 'byAssignee') trendQuery = sql.completedTrendByAssignee;
	if (selectedTab === 'byDomain') trendQuery = sql.completedTrendByDomain;

	const { data: trend, loading: trendLoading, error: trendError } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': trendQuery } },
		skip:
			!snapshot
			|| selectedTab === 'byAssignee' && !selectedAssignee
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	useEffect(() => {
		if (!percentCountLoading && !trendLoading && !stats) setLoadStates('stats', 'completedTrend', true);
	}, [percentCountLoading, trendLoading, setLoadStates, stats]);

	// States the chart can be in
	const chartState = {
		loading: percentCountLoading && trendLoading,
		error: !!percentCountError || !!trendError,
		noDataTimeframe: percentCount?.formAnalytics?.table?.length === 0,
		noData: !percentCount || !trend
	};

	// Render component to display the chart state
	if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

	const percentCountMerged = mergeRowAndHeaderData(
		percentCount?.formAnalytics?.header, percentCount?.formAnalytics?.table || []
	);

	const trendMerged = mergeRowAndHeaderData(trend?.formAnalytics?.header, trend?.formAnalytics?.table || []);
	if (percentCountMerged.length === 0) return <ChartNoData />;

	return (
		<StatusSeriesWrapper>
			<StatusSeriesHeading>
				{formatPercentage(percentCountMerged[0].completed_asset_percent || 0)} Complete
			</StatusSeriesHeading>
			<StatusSeriesDescription>
				of {parseFloat(percentCountMerged[0].assigned_asset_count || 0).toLocaleString()} assests assigned
			</StatusSeriesDescription>
			{trendMerged.length > 0 && <SimpleLineChart data={sortBy(trendMerged, ['date'])} />}
		</StatusSeriesWrapper>
	);
}

const NotStartedTrend = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { stats, setLoadStates },
		tabs: { selectedTab },
		byAssignee: { selectedAssignee },
		byDomain: { selectedDomain }
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let percentCountQuery = sql.notStartedTrendPercentAndCount;
	if (selectedTab === 'byAssignee') percentCountQuery = sql.notStartedTrendPercentAndCountByAssignee;
	if (selectedTab === 'byDomain') percentCountQuery = sql.notStartedTrendPercentAndCountByDomain;

	const { data: percentCount, loading: percentCountLoading, error: percentCountError } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': percentCountQuery } },
		skip:
			!snapshot
			|| selectedTab === 'byAssignee' && !selectedAssignee
			|| selectedTab === 'byDomain' && !selectedDomain
	});


	// Switch the query based on the selected tab
	let trendQuery = sql.notStartedTrend;
	if (selectedTab === 'byAssignee') trendQuery = sql.notStartedTrendByAssignee;
	if (selectedTab === 'byDomain') trendQuery = sql.notStartedTrendByDomain;

	const { data: trend, loading: trendLoading, error: trendError } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': trendQuery } },
		skip:
			!snapshot
			|| selectedTab === 'byAssignee' && !selectedAssignee
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	useEffect(() => {
		if (!percentCountLoading && !trendLoading && !stats) setLoadStates('stats', 'notStartedTrend', true);
	}, [percentCountLoading, trendLoading, setLoadStates, stats]);

	// States the chart can be in
	const chartState = {
		loading: percentCountLoading && trendLoading,
		error: !!percentCountError || !!trendError,
		noDataTimeframe: percentCount?.formAnalytics?.table?.length === 0,
		noData: !percentCount || !trend
	};

	// Render component to display the chart state
	if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

	const percentCountMerged = mergeRowAndHeaderData(
		percentCount?.formAnalytics?.header, percentCount?.formAnalytics?.table || []
	);

	const trendMerged = mergeRowAndHeaderData(trend?.formAnalytics?.header, trend?.formAnalytics?.table || []);
	if (percentCountMerged.length === 0) return <ChartNoData />;

	return (
		<StatusSeriesWrapper>
			<StatusSeriesHeading>
				{formatPercentage(percentCountMerged[0].completed_asset_percent || 0)} Complete
			</StatusSeriesHeading>
			<StatusSeriesDescription>
				of {parseFloat(percentCountMerged[0].assigned_asset_count || 0).toLocaleString()} assests assigned
			</StatusSeriesDescription>
			{trendMerged.length > 0 && <SimpleLineChart data={sortBy(trendMerged, ['date'])} />}
		</StatusSeriesWrapper>
	);
}

// this section is always first, so no condtional render for waterfall load
export const Stats = () => (
	<ChartGroup>
		<Row>
			<ChartCard title="Assets With Documentation" chart={<CompletedTrend />} />
			<ChartCard title="Assets Without Documentation" chart={<NotStartedTrend />} />
		</Row>
	</ChartGroup>
);