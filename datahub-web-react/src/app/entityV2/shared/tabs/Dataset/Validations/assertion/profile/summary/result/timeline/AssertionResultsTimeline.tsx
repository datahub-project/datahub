import React, { useEffect, useState } from 'react';

import styled from 'styled-components';

import { useGetAssertionRunsLazyQuery } from '../../../../../../../../../../../graphql/assertion.generated';
import { getFixedLookbackWindow } from '../../../../../../../../../../shared/time/timeUtils';
import { LOOKBACK_WINDOWS } from '../../../../../../Stats/lookbackWindows';
import { TimeSelect } from './TimeSelect';
import { AssertionResultsTimelineViz } from './AssertionResultsTimelineViz';
import { Assertion, AssertionType } from '../../../../../../../../../../../types.generated';
import { calculateInitialLookbackWindowFromRunEvents } from './utils';
import { AssertionTimelineSkeleton } from './AssertionTimelineSkeleton';
import { Message } from '../../../../../../../../../../shared/Message';

const RESULT_CHART_WIDTH_PX = 560;
const VIZ_CONTAINER_HEIGHT = 240;
const FRESHNESS_VIZ_CONTAINER_HEIGHT = 180;

const Container = styled.div`
    width: ${RESULT_CHART_WIDTH_PX}px;
`;

type Props = {
    assertion: Assertion;
};

// TODO: Add the run summary table here as well.
// TODO: here's where we will switch on the assertion itself.
export const AssertionResultsTimeline = ({ assertion }: Props) => {
    /**
     * Retrieve a specific assertion's evaluations between a particular start and end time.
     */
    const [getAssertionRuns, { data, loading, error }] = useGetAssertionRunsLazyQuery({ fetchPolicy: 'cache-first' });

    /**
     * Set default window for fetching assertion history.
     */
    const [lookbackWindow, setLookbackWindow] = useState(LOOKBACK_WINDOWS.MONTH);

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

        const maybeWindow = allRunEvents && calculateInitialLookbackWindowFromRunEvents(allRunEvents);
        if (maybeWindow) {
            setLookbackWindow(maybeWindow);
        }
        if (!loading && hasInitialDataFetchTriggered) {
            // Update initialization state on the next tick so the UI has a tick to react to the new lookback window
            setTimeout(() => setHasInitializedLookbackWindow(true), 0);
        }
    }, [allRunEvents, loading, hasInitialDataFetchTriggered, hasInitializedLookbackWindow]);

    /**
     * Whenever the selected lookback window changes (via user selection), then
     * refetch the runs for the assertion.
     */
    useEffect(() => {
        getAssertionRuns({
            variables: { assertionUrn: assertion.urn, ...getFixedLookbackWindow(lookbackWindow.windowSize) },
        });

        // Next tick so the UI has a moment to respond to the graphql query starting
        setTimeout(() => setHasInitialDataFetchTriggered(true), 0);
    }, [assertion.urn, lookbackWindow, getAssertionRuns]);

    const selectedWindow = getFixedLookbackWindow(lookbackWindow.windowSize);
    const selectedWindowTimeRange = {
        startMs: selectedWindow.startTime,
        endMs: selectedWindow.endTime,
    };
    const results = data?.assertion?.runEvents;
    const isInitializing = !hasInitializedLookbackWindow;

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
                    timeRange={selectedWindowTimeRange}
                    isInitializing={isInitializing}
                    results={results as any}
                />
            )}
            <TimeSelect lookbackWindow={lookbackWindow} setLookbackWindow={setLookbackWindow} />
        </Container>
    );
};
