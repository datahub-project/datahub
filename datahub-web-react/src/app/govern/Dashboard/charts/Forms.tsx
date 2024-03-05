import React, { useEffect } from 'react';

import { FormOutlined } from '@ant-design/icons';

import { ChartCard, HorizontalBarChart } from '../../../dataviz';
import { ChartGroup, Row, SecondaryHeading, ChartPerformanceItems, ChartPerformanceItem } from '../components';

import { statusOrdinalScale, mergeRowAndHeaderData, formatPercentage, getEntityInfo } from '../utils';
import { useFormAnalyticsQuery } from '../../../../graphql/analytics.generated';
import { useFormAnalyticsContext } from '../FormAnalyticsContext';

import { SectionWaiting, ChartState, ChartNoData, ChartNotEnoughData } from './AuxViews';

const DocumentationProgressPerForm = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { forms, setLoadStates },
		tabs: { selectedTab },
		byAssignee: { selectedAssignee },
		byDomain: { selectedDomain }
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let query = sql.overallDocProgressByForm;
	if (selectedTab === 'byAssignee') query = sql.byAssigneeOverallDocProgressByForm;
	if (selectedTab === 'byDomain') query = sql.byDomainOverallDocProgressByForm;

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': query } },
		skip:
			!snapshot
			|| selectedTab === 'byAssignee' && !selectedAssignee
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	useEffect(() => {
		if (!loading && !forms) setLoadStates('forms', 'progressByForm', true);
	}, [loading, setLoadStates, forms]);

	// States the chart can be in
	const chartState = {
		loading,
		error: !!error,
		noDataTimeframe: data?.formAnalytics?.table?.length === 0,
		noData: !data
	};

	// Render component to display the chart state
	if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

	const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []).map((d) => {
		return ({
			...d,
			form: getEntityInfo(data, d.form)?.info?.name || d.form
		});
	});
	if (mergedData.length === 0) return <ChartNoData />;

	const datakeys = Object.keys(mergedData[0]).filter((k) => k !== 'form');

	return (
		<HorizontalBarChart
			data={mergedData}
			dataKeys={datakeys}
			yAccessor={(d: { form: string }) => d.form}
			xAccessor={(d, k) => d[k]}
			colorAccessor={statusOrdinalScale}
		/>
	);
}

const DocumentationProgressTopPerforming = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { forms, setLoadStates },
		tabs: { selectedTab },
		byAssignee: { selectedAssignee },
		byDomain: { selectedDomain }
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let query = sql.overallDocProgressByFormTopPerforming;
	if (selectedTab === 'byAssignee') query = sql.byAssigneeOverallDocProgressByFormTopPerforming;
	if (selectedTab === 'byDomain') query = sql.byDomainOverallDocProgressByFormTopPerforming;

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': query } },
		skip:
			!snapshot
			|| selectedTab === 'byAssignee' && !selectedAssignee
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	useEffect(() => {
		if (!loading && !forms) setLoadStates('forms', 'formTopPerforming', true);
	}, [loading, setLoadStates, forms]);

	// States the chart can be in
	const chartState = {
		loading,
		error: !!error,
		noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
		noData: !data
	};

	// Render component to display the chart state
	if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

	const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []);
	if (mergedData.length === 0) return <ChartNoData />;

	// If there is only 1 record, show a "not enough data" alert (top and least will be the same values)
	if (mergedData.length === 1) return <ChartNotEnoughData />;

	return (
		<ChartPerformanceItems>
			{mergedData.map((d) => (
				<ChartPerformanceItem key={d.form}>
					<div><FormOutlined /> {getEntityInfo(data, d.form)?.info?.name || d.form}</div>
					{formatPercentage(d.completed_asset_percent)} completed
				</ChartPerformanceItem>
			))}
		</ChartPerformanceItems>
	)
};

const DocumentationProgressLeastPerforming = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { forms, setLoadStates },
		tabs: { selectedTab },
		byAssignee: { selectedAssignee },
		byDomain: { selectedDomain }
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let query = sql.overallDocProgressByFormLeastPerforming;
	if (selectedTab === 'byAssignee') query = sql.byAssigneeOverallDocProgressByFormLeastPerforming;
	if (selectedTab === 'byDomain') query = sql.byDomainOverallDocProgressByFormLeastPerforming;

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': query } },
		skip:
			!snapshot
			|| selectedTab === 'byAssignee' && !selectedAssignee
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	useEffect(() => {
		if (!loading && !forms) setLoadStates('forms', 'formLeastPerforming', true);
	}, [loading, setLoadStates, forms]);

	// States the chart can be in
	const chartState = {
		loading,
		error: !!error,
		noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
		noData: !data
	};

	// Render component to display the chart state
	if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

	const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []);
	if (mergedData.length === 0) return <ChartNoData />;

	// If there is only 1 record, show a "not enough data" alert (top and least will be the same values)
	if (mergedData.length === 1) return <ChartNotEnoughData />;

	return (
		<ChartPerformanceItems>
			{mergedData.map((d) => (
				<ChartPerformanceItem key={d.form}>
					<div><FormOutlined /> {getEntityInfo(data, d.form)?.info?.name || d.form}</div>
					{formatPercentage(d.completed_asset_percent)} completed
				</ChartPerformanceItem>
			))}
		</ChartPerformanceItems>
	)
};

export const Forms = () => {
	const { sectionLoadStates: { overallProgress } } = useFormAnalyticsContext();

	// This section currently always comes after the overall progress section (waterfall render)
	if (!overallProgress) return <SectionWaiting />;

	return (
		<ChartGroup>
			<SecondaryHeading>Forms</SecondaryHeading>
			<Row>
				<ChartCard
					title="Documentation progress by form"
					chart={<DocumentationProgressPerForm />}
					flex={2}
				/>
				<Row style={{ flexDirection: 'column', width: 'auto' }}>
					<ChartCard title="Top Performing Forms" chart={<DocumentationProgressTopPerforming />} />
					<ChartCard title="Under Performing Forms" chart={<DocumentationProgressLeastPerforming />} />
				</Row>
			</Row>
		</ChartGroup>
	);
};