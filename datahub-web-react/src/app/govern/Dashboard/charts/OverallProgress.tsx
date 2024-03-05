import React, { useEffect } from 'react';

import { sortBy } from 'lodash';

import { ChartCard, PieChart, BarChart } from '../../../dataviz';
import { COMPLETED_COLOR, NOT_STARTED_COLOR, IN_PROGRESS_COLOR } from '../../../dataviz/constants';

import { ChartGroup, Row, SecondaryHeading } from '../components';

import { useFormAnalyticsQuery } from '../../../../graphql/analytics.generated';
import { statusOrdinalScale, mergeRowAndHeaderData } from '../utils';
import { useFormAnalyticsContext } from '../FormAnalyticsContext';

import { SectionWaiting, ChartState } from './AuxViews';

const OverallAssignedStatusChart = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { overallProgress, setLoadStates },
		tabs: { selectedTab },
		byForm: { selectedForm },
		byAssignee: { selectedAssignee },
		byDomain: { selectedDomain }
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let query = sql.overallAssignedStatus;
	if (selectedTab === 'byForm') query = sql.byFormOverallProgress;
	if (selectedTab === 'byAssignee') query = sql.byAssigneeOverallProgress;
	if (selectedTab === 'byDomain') query = sql.byDomainOverallProgress;

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': query } },
		skip:
			!snapshot
			|| selectedTab === 'byForm' && !selectedForm
			|| selectedTab === 'byAssignee' && !selectedAssignee
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	useEffect(() => {
		if (!loading && !overallProgress) setLoadStates('overallProgress', 'assignedStatus', true);
	}, [loading, setLoadStates, overallProgress]);

	// States the chart can be in
	const chartState = {
		loading,
		error: !!error,
		noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
		noData: !data
	};

	// Render component to display the chart state
	if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

	const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table);

	const overallAssignedStatusData = [
		{ value: Number(mergedData[0].Completed), name: 'Completed', color: COMPLETED_COLOR },
		{ value: Number(mergedData[0]['In Progress']), name: 'In Progress', color: IN_PROGRESS_COLOR },
		{ value: Number(mergedData[0]['Not Started']), name: 'Not Started', color: NOT_STARTED_COLOR }
	];

	if (
		mergedData[0].Completed === '0'
		&& mergedData[0]['In Progress'] === '0'
		&& mergedData[0]['Not Started'] === '0'
	) return (<>Zero activity on forms for this time frame</>);

	return (
		<PieChart data={overallAssignedStatusData.flat()} />
	)
}

const OverallDocProgressByDate = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { overallProgress, setLoadStates },
		tabs: { selectedTab },
		byForm: { selectedForm },
		byAssignee: { selectedAssignee },
		byDomain: { selectedDomain }
	} = useFormAnalyticsContext();

	// Switch the query based on the selected tab
	let query = sql.overallDocProgressByDate;
	if (selectedTab === 'byForm') query = sql.byFormOverallProgressByDate;
	if (selectedTab === 'byAssignee') query = sql.byAssigneeOverallProgressByDate;
	if (selectedTab === 'byDomain') query = sql.byDomainOverallProgressByDate;

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': query } },
		skip:
			!snapshot
			|| selectedTab === 'byForm' && !selectedForm
			|| selectedTab === 'byAssignee' && !selectedAssignee
			|| selectedTab === 'byDomain' && !selectedDomain
	});

	useEffect(() => {
		if (!loading && !overallProgress) setLoadStates('overallProgress', 'docProgress', true);
	}, [loading, setLoadStates, overallProgress]);

	// States the chart can be in
	const chartState = {
		loading,
		error: !!error,
		noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
		noData: !data
	};

	// Render component to display the chart state
	if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

	const mergedData = sortBy(
		mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || [])
		, ['date']);

	const datakeys = Object.keys(mergedData[0]).filter((k) => k !== 'date');

	return (
		<BarChart
			data={mergedData}
			dataKeys={datakeys}
			xAccessor={(d: { date: string }) => d.date}
			yAccessor={(d, k) => d[k]}
			colorAccessor={statusOrdinalScale}
		/>
	)
}

export const OverallProgress = () => {
	const {
		tabs: { selectedTab },
		sectionLoadStates: { stats },
	} = useFormAnalyticsContext();

	// this section sometimes comes after the stats section (waterfall render)
	if (selectedTab !== 'overall' && !stats) return <SectionWaiting />;

	return (
		<ChartGroup>
			<SecondaryHeading>Overall Progress</SecondaryHeading>
			<Row>
				<ChartCard
					title="Overalled Assigned Status"
					chart={<OverallAssignedStatusChart />}
				/>
				<ChartCard
					title="Overall documentation progress by assigned date"
					chart={<OverallDocProgressByDate />}
					flex={2}
				/>
			</Row>
		</ChartGroup>
	);
}