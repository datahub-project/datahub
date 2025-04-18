import React, { useEffect } from 'react';

import { sortBy } from 'lodash';

import { ChartCard, BarChart } from '../../../dataviz';

import { ChartGroup, Row, SecondaryHeading } from '../components';

import { useFormAnalyticsQuery } from '../../../../graphql/analytics.generated';
import { statusOrdinalScale, mergeRowAndHeaderData, dateFormat } from '../utils';
import { useFormAnalyticsContext } from '../FormAnalyticsContext';

import { SectionWaiting, ChartState } from './AuxViews';

const OverallDocProgressByDate = () => {
    const {
        sql,
        timeSeries: { selectedSeries },
        sectionLoadStates: { overallProgress, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.docStatusByDate } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !overallProgress) setLoadStates('overallProgress', 'docProgress', true);
    }, [loading, setLoadStates, overallProgress]);

    // States the chart can be in
    const chartState = {
        loading,
        error: !!error,
        noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
        noData: data?.formAnalytics?.table?.length === 0,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const mergedData = sortBy(mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []), [
        'date',
    ]);

    const datakeys = mergedData[0] ? Object.keys(mergedData[0]).filter((k) => k !== 'date') : [];

    return (
        <BarChart
            data={mergedData}
            dataKeys={datakeys}
            xAccessor={(d: { date: string }) => d.date}
            yAccessor={(d, k) => d[k]}
            colorAccessor={statusOrdinalScale}
            tickFormat={dateFormat(selectedSeries)}
            yAxisLabel="# Assets"
        />
    );
};

export const OverallProgress = () => {
    const {
        sectionLoadStates: { stats },
    } = useFormAnalyticsContext();

    if (!stats) return <SectionWaiting />;

    return (
        <ChartGroup>
            <SecondaryHeading>Overall Progress</SecondaryHeading>
            <Row>
                <ChartCard title="Progress By Assigned Date" chart={<OverallDocProgressByDate />} />
            </Row>
        </ChartGroup>
    );
};
