import { scaleOrdinal } from '@visx/scale';
import React, { useEffect } from 'react';

import { ChartCard, HorizontalFullBarChart } from '@app/dataviz';
import { COMPLETED_COLOR, IN_PROGRESS_COLOR, NOT_STARTED_COLOR } from '@app/dataviz/constants';
import { useFormAnalyticsContext } from '@app/govern/Dashboard/FormAnalyticsContext';
import { ChartNoData, ChartState, SectionWaiting } from '@app/govern/Dashboard/charts/AuxViews';
import { ChartGroup, Row, SecondaryHeading } from '@app/govern/Dashboard/components';
import { mergeRowAndHeaderData, truncateString } from '@app/govern/Dashboard/utils';

import { useFormAnalyticsQuery } from '@graphql/analytics.generated';
import { useGetFormQuery } from '@graphql/form.generated';

/* THIS CHART CAN ONLY WORK ON THE FORMS TAB WHEN A FORM ID IS SELECTED */
const DocumentationProgressByQuestion = () => {
    const {
        sql,
        sectionLoadStates: { questions, setLoadStates },
        byForm: { selectedForm },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.formQuestionProgress } },
        skip: sql.skip,
    });

    // Get the form data
    const { data: formData } = useGetFormQuery({
        variables: { urn: selectedForm || '' },
        skip: !selectedForm,
    });

    // Get the prompts from the form data
    const prompts = formData?.form?.info?.prompts || [];

    useEffect(() => {
        if (!loading && !questions) setLoadStates('questions', 'questions', true);
    }, [loading, setLoadStates, questions]);

    // States the chart can be in
    const chartState = {
        loading,
        error: !!error,
        noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
        noData: data?.formAnalytics?.table?.length === 0,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []).map(
        (d) => ({
            ...d,
            question: d.question,
        }),
    );

    if (mergedData.length === 0) return <ChartNoData />;

    const docProgressByFormDataKeys = Object.keys(mergedData[0]).filter((k) => k !== 'question');

    const ordinalColorScale = scaleOrdinal({
        domain: ['Not Started', 'In Progress', 'Completed'],
        range: [NOT_STARTED_COLOR, IN_PROGRESS_COLOR, COMPLETED_COLOR],
    });

    // Add prompts to chart data (FE data hydration)
    const updatedData = mergedData.map((d) => {
        const prompt = prompts.find((p) => p.id === d.question);
        return {
            ...d,
            question: truncateString(prompt?.title) || d.question,
        };
    });

    return (
        <HorizontalFullBarChart
            data={updatedData}
            dataKeys={docProgressByFormDataKeys}
            yAccessor={(d: { question: string }) => d.question}
            colorAccessor={ordinalColorScale}
        />
    );
};

export const Questions = () => {
    const {
        sectionLoadStates: { overallProgress },
    } = useFormAnalyticsContext();

    // this section only occurs on the forms tab and currently comes after overall progress (waterfall render)
    if (!overallProgress) return <SectionWaiting />;

    return (
        <ChartGroup>
            <SecondaryHeading>Questions</SecondaryHeading>
            <Row>
                <ChartCard title="Progress by Question" chart={<DocumentationProgressByQuestion />} />
            </Row>
        </ChartGroup>
    );
};
