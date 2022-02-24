import { Tooltip, Typography } from 'antd';
import { SelectValue } from 'antd/lib/select';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useGetAssertionRunsLazyQuery } from '../../../../../../graphql/dataset.generated';
import { AssertionResultType, AssertionRunEvent } from '../../../../../../types.generated';
import { formatNumber } from '../../../../../shared/formatNumber';
import { getFixedLookbackWindow, getLocaleTimezone } from '../../../../../shared/time/timeUtils';
import { ANTD_GRAY } from '../../../constants';
import PrefixedSelect from '../Stats/historical/shared/PrefixedSelect';
import { LOOKBACK_WINDOWS } from '../Stats/lookbackWindows';
import { getResultColor, getResultIcon, getResultMessage, getResultText } from './assertionUtils';
import { BooleanTimeline } from './BooleanTimeline';

const ReportedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    urn: string;
    lastReportedAt?: number | undefined;
};

export const AssertionDetails = ({ urn, lastReportedAt }: Props) => {
    const [getAssertionRuns, { data }] = useGetAssertionRunsLazyQuery();
    const [lookbackWindow, setLookbackWindow] = useState(LOOKBACK_WINDOWS.WEEK);

    useEffect(() => {
        getAssertionRuns({
            variables: { assertionUrn: urn, ...getFixedLookbackWindow(lookbackWindow.windowSize) },
        });
    }, [urn, lookbackWindow, getAssertionRuns]);

    const onChangeLookbackWindow = (value: SelectValue) => {
        const newLookbackWindow = Object.values(LOOKBACK_WINDOWS).filter((window) => window.text === value?.valueOf());
        setLookbackWindow(newLookbackWindow[0]);
    };

    const selectedWindow = getFixedLookbackWindow(lookbackWindow.windowSize);

    const selectedWindowTimeRange = {
        startMs: selectedWindow.startTime,
        endMs: selectedWindow.endTime,
    };

    const chartData =
        data?.assertion?.runEvents?.runEvents.map((runEvent) => {
            const { result } = runEvent;

            const resultTitle = result && (
                <>
                    <span style={{ marginRight: 8 }}>{getResultIcon(result.type)}</span>
                    <Typography.Text strong>{getResultText(result.type)}</Typography.Text>
                </>
            );

            const resultMessage =
                data.assertion?.info && getResultMessage(data.assertion.info, runEvent as AssertionRunEvent);
            const resultTime = new Date(runEvent.timestampMillis);
            const localTime = resultTime.toLocaleString();
            const gmtTime = resultTime.toUTCString();

            const resultContent = (
                <>
                    <div style={{ marginBottom: 4 }}>{resultMessage}</div>
                    <div>
                        <Tooltip title={`${gmtTime}`}>
                            <Typography.Text type="secondary">{localTime}</Typography.Text>
                        </Tooltip>
                    </div>
                </>
            );

            return {
                time: runEvent.timestampMillis,
                result: {
                    title: resultTitle,
                    content: resultContent,
                    result: result?.type !== AssertionResultType.Failure,
                },
            };
        }) || [];

    const resolvedReportedAt = lastReportedAt
        ? new Date(lastReportedAt)
        : (data?.assertion?.runEvents?.runEvents.length &&
              new Date(data?.assertion?.runEvents.runEvents[0].timestampMillis)) ||
          undefined;

    const localeTimezone = getLocaleTimezone();
    const lastEvaluatedTimeLocal =
        (resolvedReportedAt &&
            `Last evaluated on ${resolvedReportedAt.toLocaleDateString()} at ${resolvedReportedAt.toLocaleTimeString()} (${localeTimezone})`) ||
        'No evaluations found';
    const lastEvaluatedTimeGMT = resolvedReportedAt && resolvedReportedAt.toUTCString();
    const startDate = new Date(selectedWindowTimeRange.startMs).toLocaleDateString('en-us', {
        month: 'short',
        day: 'numeric',
    });
    const endDate = new Date(selectedWindowTimeRange.endMs).toLocaleDateString('en-us', {
        month: 'short',
        day: 'numeric',
    });

    const succeededCount = data?.assertion?.runEvents?.succeeded;
    const failedCount = data?.assertion?.runEvents?.failed;

    return (
        <div style={{ width: '100%', paddingLeft: 52 }}>
            <div>
                <Typography.Title level={5}>Evaluations</Typography.Title>
                <div style={{ width: '100%', display: 'flex', justifyContent: 'space-between' }}>
                    <Tooltip title={lastEvaluatedTimeGMT}>
                        <ReportedAtLabel>{lastEvaluatedTimeLocal}</ReportedAtLabel>
                    </Tooltip>
                </div>
                <div style={{ width: 800, paddingTop: 12 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                        <div
                            style={{
                                width: 250,
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'left',
                            }}
                        >
                            <Typography.Text strong style={{ marginRight: 20 }}>
                                {startDate} - {endDate}
                            </Typography.Text>
                            <div>
                                <span style={{ marginRight: 12 }}>
                                    <Typography.Text
                                        style={{ color: getResultColor(AssertionResultType.Success), fontWeight: 600 }}
                                    >
                                        {formatNumber(succeededCount)}
                                    </Typography.Text>{' '}
                                    passed
                                </span>
                                <span>
                                    <Typography.Text
                                        style={{ color: getResultColor(AssertionResultType.Failure), fontWeight: 600 }}
                                    >
                                        {formatNumber(failedCount)}
                                    </Typography.Text>{' '}
                                    failed
                                </span>
                            </div>
                        </div>

                        <PrefixedSelect
                            prefixText="Show "
                            values={Object.values(LOOKBACK_WINDOWS).map((window) => window.text)}
                            value={lookbackWindow.text}
                            setValue={onChangeLookbackWindow}
                        />
                    </div>
                    <BooleanTimeline width={800} data={chartData} timeRange={selectedWindowTimeRange} />
                </div>
            </div>
        </div>
    );
};
