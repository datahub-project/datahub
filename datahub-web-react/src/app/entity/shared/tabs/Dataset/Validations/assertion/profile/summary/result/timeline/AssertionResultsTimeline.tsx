import React, { useEffect, useState } from 'react';

import styled from 'styled-components';
import { Typography } from 'antd';

import { useGetAssertionRunsLazyQuery } from '../../../../../../../../../../../graphql/assertion.generated';
import { getFixedLookbackWindow } from '../../../../../../../../../../shared/time/timeUtils';
import { LOOKBACK_WINDOWS } from '../../../../../../Stats/lookbackWindows';
import { TimeSelect } from './TimeSelect';
import { AssertionResultsTimelineViz } from './AssertionResultsTimelineViz';
import { ANTD_GRAY } from '../../../../../../../../constants';
import { Assertion } from '../../../../../../../../../../../types.generated';
import { getTimeRangeDisplay } from './utils';

const RESULT_CHART_WIDTH_PX = 560;
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
    const [getAssertionRuns, { data }] = useGetAssertionRunsLazyQuery({ fetchPolicy: 'cache-first' });

    /**
     * Set default window for fetching assertion history.
     */
    const [lookbackWindow, setLookbackWindow] = useState(LOOKBACK_WINDOWS.WEEK);

    /**
     * Whenever the selected lookback window changes (via user selection), then
     * refetch the runs for the assertion.
     */
    useEffect(() => {
        getAssertionRuns({
            variables: { assertionUrn: assertion.urn, ...getFixedLookbackWindow(lookbackWindow.windowSize) },
        });
    }, [assertion.urn, lookbackWindow, getAssertionRuns]);

    const selectedWindow = getFixedLookbackWindow(lookbackWindow.windowSize);
    const selectedWindowTimeRange = {
        startMs: selectedWindow.startTime,
        endMs: selectedWindow.endTime,
    };
    const results = data?.assertion?.runEvents;

    return (
        <Container>
            <AssertionResultsTimelineViz
                parentDimensions={{
                    width: RESULT_CHART_WIDTH_PX,
                }}
                assertion={assertion}
                timeRange={selectedWindowTimeRange}
                results={results as any}
                platform={data?.assertion?.platform as any}
            />
            <TimeSelect
                lookbackWindow={lookbackWindow}
                setLookbackWindow={setLookbackWindow}
            />
        </Container>
    );
};
