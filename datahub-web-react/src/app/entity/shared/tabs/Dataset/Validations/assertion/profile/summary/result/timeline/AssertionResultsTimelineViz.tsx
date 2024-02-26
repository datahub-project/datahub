import React from 'react';

import {
    Assertion,
    AssertionRunEventsResult,
    AssertionRunStatus,
    DataPlatform,
} from '../../../../../../../../../../../types.generated';
import { AssertionDataPoint, AssertionResultTimelineChart, TimeRange } from './AssertionResultTimelineChart';
import { AssertionResultPopoverContent } from '../../../shared/result/AssertionResultPopoverContent';
import { getAssertionDataPointsFromRunEvents } from './utils';

const RESULT_CHART_WIDTH_PX = 540;

type Props = {
    assertion: Assertion;
    timeRange: TimeRange;
    results?: AssertionRunEventsResult | null;
    platform?: DataPlatform | null;
};

export const AssertionResultsTimelineViz = ({ assertion, results, platform, timeRange }: Props) => {
    const completedRuns =
        results?.runEvents.filter((runEvent) => runEvent.status === AssertionRunStatus.Complete) || [];

    /**
     * Data for the timeline of assertion results.
     */
    const timelineData: AssertionDataPoint[] = getAssertionDataPointsFromRunEvents(completedRuns, {
        getPopOverContent: (runEvent) => <AssertionResultPopoverContent
            assertion={assertion}
            run={runEvent}
        />,
        getTitleElement: () => undefined,
    })

    return <AssertionResultTimelineChart width={RESULT_CHART_WIDTH_PX} data={timelineData} timeRange={timeRange} />;
};
