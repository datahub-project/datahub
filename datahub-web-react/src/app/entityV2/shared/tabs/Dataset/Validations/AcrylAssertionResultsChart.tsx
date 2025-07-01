import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { LOOKBACK_WINDOWS } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import { AcrylAssertionResultsChartHeader } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionResultsChartHeader';
import { AcrylAssertionResultsChartTimeline } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionResultsChartTimeline';
import { getFixedLookbackWindow } from '@app/shared/time/timeUtils';

import { useGetAssertionRunsLazyQuery } from '@graphql/assertion.generated';

const RESULT_CHART_WIDTH_PX = 800;

const Container = styled.div`
    width: ${RESULT_CHART_WIDTH_PX}px;
    padding-top: 12px;
`;

type Props = {
    urn: string;
};

export const AcrylAssertionResultsChart = ({ urn }: Props) => {
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
            variables: { assertionUrn: urn, ...getFixedLookbackWindow(lookbackWindow.windowSize) },
        });
    }, [urn, lookbackWindow, getAssertionRuns]);

    const selectedWindow = getFixedLookbackWindow(lookbackWindow.windowSize);
    const selectedWindowTimeRange = {
        startMs: selectedWindow.startTime,
        endMs: selectedWindow.endTime,
    };

    return (
        <Container>
            <AcrylAssertionResultsChartHeader
                timeRange={selectedWindowTimeRange}
                results={data?.assertion?.runEvents as any}
                lookbackWindow={lookbackWindow}
                setLookbackWindow={setLookbackWindow}
            />
            <AcrylAssertionResultsChartTimeline
                timeRange={selectedWindowTimeRange}
                results={data?.assertion?.runEvents as any}
                platform={data?.assertion?.platform as any}
            />
        </Container>
    );
};
