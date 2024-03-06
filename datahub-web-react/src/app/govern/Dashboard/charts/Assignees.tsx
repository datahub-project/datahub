import React, { useEffect } from 'react';

import { Table } from 'antd';
import { UserOutlined } from '@ant-design/icons';

import { ChartGroup, Row, SecondaryHeading, ChartPerformanceItems, ChartPerformanceItem } from '../components';
import { ChartCard } from '../../../dataviz';

import { useFormAnalyticsQuery } from '../../../../graphql/analytics.generated';
import { mergeRowAndHeaderData, getEntityInfo, formatPercentage } from '../utils';
import { useFormAnalyticsContext } from '../FormAnalyticsContext';

import { SectionWaiting, ChartState, ChartNoData, ChartNotEnoughData } from './AuxViews';

const DocProgressByAssignee = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { assignees, setLoadStates },
		tabs: { selectedTab },
		byForm: { selectedForm },
		byDomain: { selectedDomain }
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let query = sql.overallDocProgressByAssignee;
	if (selectedTab === 'byForm') query = sql.byFormOverallDocProgressByAssignee;
	if (selectedTab === 'byDomain') query = sql.byDomainOverallDocProgressByAssignee;

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': query } },
		skip:
			!snapshot
			|| selectedTab === 'byForm' && !selectedForm
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	useEffect(() => {
		if (!loading && !assignees) setLoadStates('assignees', 'progressByAssignee', true);
	}, [loading, setLoadStates, assignees]);

	// States the chart can be in
	const chartState = {
		loading,
		error: !!error,
		noDataTimeframe: !loading && !!data && data?.formAnalytics?.table?.length === 0,
		noData: !data
	};

	// Render component to display the chart state
	if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

	const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || [])
		.map((row) => {
			const { username, properties } = getEntityInfo(data, row.assignee_urn) || {};
			return ({
				Name: properties?.displayName || username || row.assignee_urn,
				Completed: Number(row.Completed),
				'In Progress': Number(row['In Progress']),
				'Not Started': Number(row['Not Started']),
				Total: Number(row.Completed) + Number(row['In Progress']) + Number(row['Not Started']),
			})
		});

	if (mergedData.length === 0) return <ChartNoData />;

	const columns = Object.keys(mergedData[0]).map((key) => ({
		key,
		dataIndex: key,
		title: key,
		sorter: (a, b) => a[key] - b[key],
	}));

	return (
		<div style={{ width: '100%', marginTop: '1rem' }}>
			{mergedData.length} total assignees
			<Table dataSource={mergedData} columns={columns} size="small" style={{ width: '100%' }} />
		</div>
	)
}

const DocProgressByAssigneeTopPerforming = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { assignees, setLoadStates },
		tabs: { selectedTab },
		byForm: { selectedForm },
		byDomain: { selectedDomain }
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let query = sql.overallDocProgressByAssigneeTopPerforming;
	if (selectedTab === 'byForm') query = sql.byFormDocProgressByAssigneeTopPerforming;
	if (selectedTab === 'byDomain') query = sql.byDomainDocProgressByAssigneeTopPerforming;

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': query } },
		skip:
			!snapshot
			|| selectedTab === 'byForm' && !selectedForm
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	useEffect(() => {
		if (!loading && !assignees) setLoadStates('assignees', 'assigneeTopPerforming', true);
	}, [loading, setLoadStates, assignees]);

	// States the chart can be in
	const chartState = {
		loading,
		error: !!error,
		noDataTimeframe: !loading && !!data && data?.formAnalytics?.table?.length === 0,
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
			{mergedData.map((d) => {
				const { username, properties } = getEntityInfo(data, d.assignee_urn) || {};
				return (
					<ChartPerformanceItem key={d.assignee_urn}>
						<div><UserOutlined /> {properties?.displayName || username || d.assignee_urn}</div>
						{formatPercentage(d.completed_asset_percent)} completed
					</ChartPerformanceItem>
				)
			})}
		</ChartPerformanceItems>
	);
};

const DocProgressByAssigneeLeastPerforming = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { assignees, setLoadStates },
		tabs: { selectedTab },
		byForm: { selectedForm },
		byDomain: { selectedDomain }
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let query = sql.overallDocProgressByAssigneeLeastPerforming;
	if (selectedTab === 'byForm') query = sql.byFormDocProgressByAssigneeLeastPerforming;
	if (selectedTab === 'byDomain') query = sql.byDomainDocProgressByAssigneeLeastPerforming;

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': query } },
		skip:
			!snapshot
			|| selectedTab === 'byForm' && !selectedForm
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	useEffect(() => {
		if (!loading && !assignees) setLoadStates('assignees', 'assigneeLeastPerforming', true);
	}, [loading, setLoadStates, assignees]);

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
			{mergedData.map((d) => {
				const { username, properties } = getEntityInfo(data, d.assignee_urn) || {};
				return (
					<ChartPerformanceItem key={d.assignee_urn}>
						<div><UserOutlined /> {properties?.displayName || username || d.assignee_urn}</div>
						{formatPercentage(d.completed_asset_percent)} completed
					</ChartPerformanceItem>
				)
			})}
		</ChartPerformanceItems>
	);
};

export const Assignees = () => {
	const {
		tabs: { selectedTab },
		sectionLoadStates: { forms, questions },
	} = useFormAnalyticsContext();

	// on the forms tab, this section comes after queestions but 
	// it's after the forms section otherwise (waterfall render)
	if (selectedTab === 'byForm' && !questions) return <SectionWaiting />;
	if (selectedTab !== 'byForm' && !forms) return <SectionWaiting />;

	return (
		<ChartGroup>
			<SecondaryHeading>Assignees</SecondaryHeading>
			<Row>
				<Row style={{ flexDirection: 'column', width: 'auto' }}>
					<ChartCard title="Top Performing Owners" chart={<DocProgressByAssigneeTopPerforming />} />
					<ChartCard title="Under Performing Owners" chart={<DocProgressByAssigneeLeastPerforming />} />
				</Row>
				<ChartCard title="Documentation progress by assignee" chart={<DocProgressByAssignee />} flex={2} />
			</Row>
		</ChartGroup>
	)
};