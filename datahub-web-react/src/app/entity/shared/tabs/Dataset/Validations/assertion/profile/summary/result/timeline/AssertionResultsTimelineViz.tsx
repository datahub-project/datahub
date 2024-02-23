import React from 'react';

import {
    Assertion,
    AssertionRunEventsResult,
    AssertionRunStatus,
    DataPlatform,
} from '../../../../../../../../../../../types.generated';
import { AssertionDataPoint, AssertionResultTimelineChart, TimeRange } from './AssertionResultTimelineChart';
import { AssertionResultPopoverContent } from '../../../shared/result/AssertionResultPopoverContent';

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
    const timelineData: AssertionDataPoint[] =
        completedRuns
            .filter((runEvent) => !!runEvent.result)
            .map((runEvent) => {
                const { result } = runEvent;
                if (!result) throw new Error('Completed assertion run event does not have a result.');
                const resultUrl = result.externalUrl;
                /**
                 * Create a "result" to render in the timeline chart.
                 */
                return {
                    time: runEvent.timestampMillis,
                    result: {
                        type: result.type,
                        resultUrl,
                        title: undefined,
                        popoverContent: (
                            <AssertionResultPopoverContent
                                assertion={assertion}
                                run={runEvent}
                            />
                        ),
                    },
                };
            }) || [];

    return <AssertionResultTimelineChart width={RESULT_CHART_WIDTH_PX} data={timelineData} timeRange={timeRange} />;
};
