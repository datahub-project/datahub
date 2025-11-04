import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import { LOOKBACK_WINDOWS } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import { AssertionResultsTimelineViz } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/AssertionResultsTimelineViz';
import { AssertionTimelineSkeleton } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/AssertionTimelineSkeleton';
import {
    CUSTOM_TIME_RANGE_WINDOW,
    TimeSelect,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/TimeSelect';
import { calculateInitialLookbackWindowFromRunEvents } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/utils';
import { Message } from '@app/shared/Message';
import { getFixedLookbackWindow } from '@app/shared/time/timeUtils';

import { useGetAssertionRunsLazyQuery } from '@graphql/assertion.generated';
import { Assertion, AssertionType, Maybe, Monitor } from '@types';

const RESULT_CHART_WIDTH_PX = 560;
const VIZ_CONTAINER_HEIGHT = 260;
const FRESHNESS_VIZ_CONTAINER_HEIGHT = 180;

const Container = styled.div`
    width: ${RESULT_CHART_WIDTH_PX}px;
`;

type Props = {
    assertion: Assertion;
    monitor?: Maybe<Monitor>;
    openAssertionNote?: () => void;
    refreshData?: () => Promise<unknown>;
};

// TODO: Add the run summary table here as well.
// TODO: here's where we will switch on the assertion itself.
export const AssertionResultsTimeline = ({ assertion, monitor, openAssertionNote, refreshData }: Props) => {
    /**
     * Retrieve a specific assertion's evaluations between a particular start and end time.
     */
    const [getAssertionRuns, { data, loading, error, refetch: refetchRuns }] = useGetAssertionRunsLazyQuery({
        fetchPolicy: 'cache-first',
    });

    const refetch = () => {
        return Promise.allSettled([refetchRuns(), refreshData?.()]);
    };

    /**
     * Set default window for fetching assertion history.
     */
    const [selectedTimeWindow, setSelectedTimeWindow] = useState({
        window: getFixedLookbackWindow(LOOKBACK_WINDOWS.MONTH.windowSize),
        windowName: LOOKBACK_WINDOWS.MONTH.text,
    });

    /**
     * Track whether we've triggered a data fetch yet
     */
    const [hasInitialDataFetchTriggered, setHasInitialDataFetchTriggered] = useState(false);

    /**
     * On initial load, set the lookback window to a reasonable scale
     */
    const [hasInitializedLookbackWindow, setHasInitializedLookbackWindow] = useState(false);
    const allRunEvents = data?.assertion?.runEvents?.runEvents;
    useEffect(() => {
        if (hasInitializedLookbackWindow) return;

        const maybeWindow = allRunEvents && calculateInitialLookbackWindowFromRunEvents(allRunEvents, monitor);
        if (maybeWindow) {
            setSelectedTimeWindow({
                window: getFixedLookbackWindow(maybeWindow.windowSize),
                windowName: maybeWindow.text,
            });
        }
        if (!loading && hasInitialDataFetchTriggered) {
            // Update initialization state on the next tick so the UI has a tick to react to the new lookback window
            setTimeout(() => setHasInitializedLookbackWindow(true), 0);
        }
    }, [allRunEvents, monitor, loading, hasInitialDataFetchTriggered, hasInitializedLookbackWindow]);

    /**
     * Whenever the selected lookback window changes (via user selection), then
     * refetch the runs for the assertion.
     */
    useEffect(() => {
        getAssertionRuns({
            variables: { assertionUrn: assertion.urn, ...selectedTimeWindow.window },
        });

        // Next tick so the UI has a moment to respond to the graphql query starting
        setTimeout(() => setHasInitialDataFetchTriggered(true), 0);
    }, [assertion.urn, selectedTimeWindow, getAssertionRuns]);

    const selectedWindowTimeRange = {
        startMs: selectedTimeWindow.window.startTime,
        endMs: selectedTimeWindow.window.endTime,
    };
    const results = data?.assertion?.runEvents;
    const isInitializing = !hasInitializedLookbackWindow;

    // Handle time range selection from chart drag
    const handleTimeRangeChange = useCallback(
        (startTimeMs: number, endTimeMs: number) => {
            setSelectedTimeWindow({
                window: {
                    startTime: startTimeMs,
                    endTime: endTimeMs,
                },
                windowName: CUSTOM_TIME_RANGE_WINDOW.windowName,
            });
        },
        [setSelectedTimeWindow],
    );

    const vizHeight =
        assertion.info?.type === AssertionType.Freshness ? FRESHNESS_VIZ_CONTAINER_HEIGHT : VIZ_CONTAINER_HEIGHT;
    return (
        <Container>
            {error && <Message type="error" content="Failed to load results! An unexpected error occurred." />}
            {loading || isInitializing ? (
                <AssertionTimelineSkeleton
                    parentDimensions={{
                        width: RESULT_CHART_WIDTH_PX,
                        height: vizHeight,
                    }}
                />
            ) : (
                <AssertionResultsTimelineViz
                    parentDimensions={{
                        width: RESULT_CHART_WIDTH_PX,
                        height: vizHeight,
                    }}
                    assertion={assertion}
                    monitor={monitor}
                    timeRange={selectedWindowTimeRange}
                    isInitializing={isInitializing}
                    results={results as any}
                    refreshData={refetch}
                    openAssertionNote={openAssertionNote}
                    onTimeRangeChange={handleTimeRangeChange}
                />
            )}
            <TimeSelect timeWindow={selectedTimeWindow} setTimeWindow={setSelectedTimeWindow} />
        </Container>
    );
};
