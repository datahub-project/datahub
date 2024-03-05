import React, { useEffect } from 'react';

import { scaleOrdinal } from "@visx/scale";

import { ChartCard, HorizontalFullBarChart } from '../../../dataviz';
import { COMPLETED_COLOR, NOT_STARTED_COLOR, IN_PROGRESS_COLOR } from '../../../dataviz/constants';

import { ChartGroup, Row, SecondaryHeading } from '../components';
import { mergeRowAndHeaderData } from '../utils';

import { useFormAnalyticsQuery } from '../../../../graphql/analytics.generated';
import { useFormAnalyticsContext } from '../FormAnalyticsContext';

import { SectionWaiting, ChartState, ChartNoData } from './AuxViews';

const DocumentationProgressByQuestion = () => {
	const {
		sql,
		snapshot,
		sectionLoadStates: { questions, setLoadStates },
		byForm: { selectedForm }
	} = useFormAnalyticsContext();

	const { data, loading, error } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': sql.byFormByQuestionProgress } },
		skip: !snapshot || !selectedForm
	});

	useEffect(() => {
		if (!loading && !questions) setLoadStates('questions', 'questions', true);
	}, [loading, setLoadStates, questions]);

	// States the chart can be in
	const chartState = {
		loading,
		error: !!error,
		noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
		noData: !data
	};

	// Render component to display the chart state
	if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

	const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []).map((d) => ({
		...d,
		question: d.question
	}));

	if (mergedData.length === 0) return <ChartNoData />;

	const docProgressByFormDataKeys = Object.keys(mergedData[0]).filter((k) =>
		k !== 'question'
	);

	const ordinalColorScale = scaleOrdinal({
		domain: ['Not Started', 'In Progress', 'Completed'],
		range: [NOT_STARTED_COLOR, IN_PROGRESS_COLOR, COMPLETED_COLOR]
	});

	return (
		<HorizontalFullBarChart
			data={mergedData}
			dataKeys={docProgressByFormDataKeys}
			yAccessor={(d: { question: string }) => d.question}
			xAccessor={(d, k) => d[k]}
			colorAccessor={ordinalColorScale}
		/>
	);
}

export const Questions = () => {
	const { sectionLoadStates: { overallProgress } } = useFormAnalyticsContext();

	// this section only occurs on the forms tab and currently comes after overall progress (waterfall render)
	if (!overallProgress) return <SectionWaiting />;

	return (
		<ChartGroup>
			<SecondaryHeading>Questions</SecondaryHeading>
			<Row>
				<ChartCard
					title="Marketing Documentation Form Progress by Question"
					chart={<DocumentationProgressByQuestion />}
				/>
			</Row>
		</ChartGroup>
	);
}