import React from 'react';

import {
    Assertion,
    AssertionRunEventsResult,
    AssertionRunStatus,
    DataPlatform,
} from '../../../../../../../../../../../types.generated';
import { ValuesOverTimeAssertionResultChart } from './charts/ValuesOverTimeAssertionResultChart';
import { AssertionDataPoint, AssertionResultChartData, TimeRange } from './types';
import { getAssertionDataPointsFromRunEvents, getAssertionResultChartData } from './transformers';
import { AssertionChartType, getBestChartTypeForAssertion } from './charts/constants';
import { StatusOverTimeAssertionResultChart } from './charts/StatusOverTimeAssertionResultChart';

type Props = {
    assertion: Assertion;
    timeRange: TimeRange;
    results?: AssertionRunEventsResult | null;
    platform?: DataPlatform | null;
    dimensions: { width: number, height: number }
};

export const AssertionResultsTimelineViz = ({ assertion, results, timeRange, dimensions }: Props) => {
    const completedRuns =
        results?.runEvents.filter((runEvent) => runEvent.status === AssertionRunStatus.Complete) || [];

    console.log(completedRuns)
    const assertionResultChartData: AssertionResultChartData = getAssertionResultChartData(
        assertion,
        completedRuns,
    )

    return getBestChartTypeForAssertion(assertion.info) == AssertionChartType.ValuesOverTime
        ? <ValuesOverTimeAssertionResultChart
            chartDimensions={dimensions}
            data={assertionResultChartData}
            timeRange={timeRange}
        />
        : <StatusOverTimeAssertionResultChart
            chartDimensions={dimensions}
            data={assertionResultChartData}
            timeRange={timeRange}
        />;
};
