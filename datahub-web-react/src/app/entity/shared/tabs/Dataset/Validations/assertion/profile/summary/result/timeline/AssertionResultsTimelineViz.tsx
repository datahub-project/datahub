import React from 'react';

import {
    Assertion,
    AssertionRunEventsResult,
    AssertionRunStatus,
    DataPlatform,
} from '../../../../../../../../../../../types.generated';
import { TimeAndValueBasedAssertionResultChart } from './charts/TimeAndValueBasedAssertionResultChart';
import { AssertionDataPoint, TimeRange } from './types';
import { getAssertionDataPointsFromRunEvents } from './transformers';
import { TIME_AND_VALUE_BASED_ASSERTION_TYPES } from './constants';
import { TimeBasedAssertionResultChart } from './charts/TimeBasedAssertionResultChart';
import ParentSize from '@visx/responsive/lib/components/ParentSize'

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

    const timelineDataPoints: AssertionDataPoint[] = getAssertionDataPointsFromRunEvents(completedRuns)

    return assertion.info?.type && TIME_AND_VALUE_BASED_ASSERTION_TYPES.includes(assertion.info.type) ? <TimeAndValueBasedAssertionResultChart
        chartDimensions={dimensions}
        data={{
            dataPoints: timelineDataPoints,
            context: {
                assertion,
            }
        }}
        timeRange={timeRange}
    /> : <TimeBasedAssertionResultChart
        chartDimensions={dimensions}
        data={{
            dataPoints: timelineDataPoints,
            context: {
                assertion,
            }
        }}
        timeRange={timeRange}
    />;
};
