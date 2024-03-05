import React, { useEffect } from 'react';

import { ChartCard, HorizontalBarChart } from '../../../dataviz';
import { ChartGroup, Row, SecondaryHeading, ChartPerformanceItems, ChartPerformanceItem } from '../components';

import { statusOrdinalScale, mergeRowAndHeaderData, formatPercentage, getEntityInfo } from '../utils';
import { useFormAnalyticsQuery } from '../../../../graphql/analytics.generated';
import { useFormAnalyticsContext } from '../FormAnalyticsContext';

import DomainIcon from '../../../domain/DomainIcon';

import { SectionWaiting, ChartState, ChartNoData, ChartNotEnoughData } from './AuxViews';

const DocumentationProgressPerDomain = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { domains, setLoadStates },
		tabs: { selectedTab },
		byForm: { selectedForm },
		byAssignee: { selectedAssignee },
		byDomain: { selectedDomain }
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let query = sql.overallDocProgressByDomain;
	if (selectedTab === 'byForm') query = sql.byFormOverallDocProgressByDomain;
	if (selectedTab === 'byAssignee') query = sql.byAssigneeOverallDocProgressByDomain;
	if (selectedTab === 'byDomain') query = sql.byDomainOverallDocProgressByDomain;

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': query } },
		skip:
			!snapshot
			|| selectedTab === 'byForm' && !selectedForm
			|| selectedTab === 'byAssignee' && !selectedAssignee
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	useEffect(() => {
		if (!loading && !domains) setLoadStates('domains', 'progressByDomain', true);
	}, [loading, setLoadStates, domains]);

	// States the chart can be in
	const chartState = {
		loading,
		error: !!error,
		noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
		noData: !data
	};

	// Render component to display the chart state
	if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

	const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []).map((d) => {
		return ({
			...d,
			domain: getEntityInfo(data, d.domain)?.properties?.name || d.domain
		});
	});
	if (mergedData.length === 0) return <ChartNoData />;

	const datakeys = Object.keys(mergedData[0]).filter((k) => k !== 'domain');

	return (
		<HorizontalBarChart
			data={mergedData}
			dataKeys={datakeys}
			yAccessor={(d: { domain: string }) => d.domain}
			xAccessor={(d, k) => d[k]}
			colorAccessor={statusOrdinalScale}
		/>
	);
}

const DocProgressByDomainTopPerforming = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { domains, setLoadStates },
		tabs: { selectedTab },
		byForm: { selectedForm },
		byAssignee: { selectedAssignee },
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let query = sql.overallDocProgressByDomainTopPerforming;
	if (selectedTab === 'byForm') query = sql.byFormDocProgressByDomainTopPerforming;
	if (selectedTab === 'byAssignee') query = sql.byAssigneeDocProgressByDomainTopPerforming;

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': query } },
		skip:
			!snapshot
			|| selectedTab === 'byForm' && !selectedForm
			|| selectedTab === 'byAssignee' && !selectedAssignee
	});

	useEffect(() => {
		if (!loading && !domains) setLoadStates('domains', 'domainTopPerforming', true);
	}, [loading, setLoadStates, domains]);

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
				<ChartPerformanceItem key={d.domain}>
					<div><DomainIcon /> {getEntityInfo(data, d.domain)?.properties?.name || d.domain}</div>
					{formatPercentage(d.completed_asset_percent)} completed
				</ChartPerformanceItem>
			))}
		</ChartPerformanceItems>
	);
};

const DocProgressByDomainLeastPerforming = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { domains, setLoadStates },
		tabs: { selectedTab },
		byForm: { selectedForm },
		byAssignee: { selectedAssignee },
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let query = sql.overallDocProgressByDomainLeastPerforming;
	if (selectedTab === 'byForm') query = sql.byFormDocProgressByDomainLeastPerforming;
	if (selectedTab === 'byAssignee') query = sql.byAssigneeDocProgressByDomainLeastPerforming;

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': query } },
		skip:
			!snapshot
			|| selectedTab === 'byForm' && !selectedForm
			|| selectedTab === 'byAssignee' && !selectedAssignee
	});

	useEffect(() => {
		if (!loading && !domains) setLoadStates('domains', 'domainLeastPerforming', true);
	}, [loading, setLoadStates, domains]);

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
				<ChartPerformanceItem key={d.domain}>
					<div><DomainIcon /> {getEntityInfo(data, d.domain)?.properties?.name || d.domain}</div>
					{formatPercentage(d.completed_asset_percent)} completed
				</ChartPerformanceItem>
			))}
		</ChartPerformanceItems>
	);
};

export const Domains = () => {
	const {
		tabs: { selectedTab },
		sectionLoadStates: { forms, assignees },
	} = useFormAnalyticsContext();

	// on the assingee tab, this section comes after forms but
	// it's after the assignee section otherwise (waterfall render)
	if (selectedTab === 'byAssignee' && !forms) return <SectionWaiting />;
	if (selectedTab !== 'byAssignee' && !assignees) return <SectionWaiting />;

	return (
		<ChartGroup>
			<SecondaryHeading>Domains</SecondaryHeading>
			<Row>
				<ChartCard
					title="Documentation progress by domain"
					chart={<DocumentationProgressPerDomain />}
					flex={2}
				/>
				<Row style={{ flexDirection: 'column', width: 'auto' }}>
					<ChartCard title="Top Performing Domains" chart={<DocProgressByDomainTopPerforming />} />
					<ChartCard title="Under Performing Domains" chart={<DocProgressByDomainLeastPerforming />} />
				</Row>
			</Row>
		</ChartGroup>
	);
}