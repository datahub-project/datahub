import { SelectValue } from 'antd/lib/select';
import React, { useEffect, useState } from 'react';
import { useGetAssertionRunsLazyQuery } from '../../../../../../graphql/dataset.generated';
import {
    AssertionInfo,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
} from '../../../../../../types.generated';
import { getFixedLookbackWindow } from '../../../../../shared/time/timeUtils';
import PrefixedSelect from '../Stats/historical/shared/PrefixedSelect';
import { LOOKBACK_WINDOWS } from '../Stats/lookbackWindows';
import { BooleanTimeline } from './BooleanTimeline';

type Props = {
    urn: string;
};

const computeResultMessage = (assertionInfo: AssertionInfo, runEvent: AssertionRunEvent) => {
    console.log(assertionInfo);
    console.log(runEvent);
    return 'Yay';
};

export const AssertionDetails = ({ urn }: Props) => {
    const [getAssertionRuns, { data }] = useGetAssertionRunsLazyQuery();
    const [lookbackWindow, setLookbackWindow] = useState(LOOKBACK_WINDOWS.MONTH);

    useEffect(() => {
        getAssertionRuns({
            variables: { assertionUrn: urn, ...getFixedLookbackWindow(lookbackWindow.windowSize) },
        });
    }, [urn, lookbackWindow, getAssertionRuns]);

    const selectedWindow = getFixedLookbackWindow(lookbackWindow.windowSize);

    // Compute Assertion Graph Data
    const timeRange = {
        startMs: selectedWindow.startTime,
        endMs: selectedWindow.endTime,
    };

    const chartData =
        (data &&
            data.assertion?.runEvents
                ?.filter((runEvent) => runEvent.status === AssertionRunStatus.Complete)
                .map((runEvent) => {
                    const { result } = runEvent;
                    const resultMessage =
                        data.assertion?.info &&
                        computeResultMessage(data.assertion?.info, runEvent as AssertionRunEvent); // Message depends on the assertion type itself.
                    const resultTitle = result?.type === AssertionResultType.Success ? 'Succeeded' : 'Failed';
                    return {
                        time: runEvent.timestampMillis,
                        result: {
                            title: resultTitle,
                            content: resultMessage,
                            result: result?.type !== AssertionResultType.Failure,
                        },
                    };
                })) ||
        [];

    console.log(chartData);

    const onChangeLookbackWindow = (value: SelectValue) => {
        const newLookbackWindow = Object.values(LOOKBACK_WINDOWS).filter((window) => window.text === value?.valueOf());
        setLookbackWindow(newLookbackWindow[0]);
    };

    return (
        <>
            <PrefixedSelect
                prefixText="Show past "
                values={Object.values(LOOKBACK_WINDOWS).map((window) => window.text)}
                value={lookbackWindow.text}
                setValue={onChangeLookbackWindow}
            />
            <BooleanTimeline data={chartData} timeRange={timeRange} />
        </>
    );
};
