import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { SelectValue } from 'antd/lib/select';
import { AssertionResultType, AssertionRunEventsResult } from '../../../../../../types.generated';
import { formatNumber } from '../../../../../shared/formatNumber';
import PrefixedSelect from '../Stats/historical/shared/PrefixedSelect';
import { LookbackWindow, LOOKBACK_WINDOWS } from '../Stats/lookbackWindows';
import { getResultColor } from './assertionUtils';
import { TimeRange } from './AssertionResultTimeline';

const Header = styled.div`
    display: flex;
    justify-content: space-between;
`;

const EvaluationsSummary = styled.div`
    width: 300px;
    display: flex;
    align-items: center;
    justify-content: left;
`;

const EvaluationDateRange = styled(Typography.Text)`
    margin-right: 20px;
`;

const EvaluationResults = styled.div``;

const ResultTypeCount = styled.span`
    margin-right: 12px;
`;

const ErrorEvaluationsCount = styled(Typography.Text)`
    font-weight: 600;
    color: ${getResultColor(AssertionResultType.Error)};
`;

type Props = {
    timeRange: TimeRange;
    results?: AssertionRunEventsResult | null;
    lookbackWindow: LookbackWindow;
    setLookbackWindow: (newWindow: LookbackWindow) => void;
};

export const AcrylAssertionResultsChartHeader = ({ timeRange, lookbackWindow, setLookbackWindow, results }: Props) => {
    /**
     * Start and end dates being observed in the chart.
     */
    const startDate = new Date(timeRange.startMs).toLocaleDateString('en-us', {
        month: 'short',
        day: 'numeric',
    });
    const endDate = new Date(timeRange.endMs).toLocaleDateString('en-us', {
        month: 'short',
        day: 'numeric',
    });

    /**
     * Success / Failure summary
     */
    const succeededCount = results?.succeeded;
    const failedCount = results?.failed;
    const errorCount = results?.errored;

    /**
     * Invoked when user selects new lookback window (e.g. 1 year)
     */
    const onChangeLookbackWindow = (value: SelectValue) => {
        const newLookbackWindow = Object.values(LOOKBACK_WINDOWS).filter((window) => window.text === value?.valueOf());
        setLookbackWindow(newLookbackWindow[0]);
    };

    return (
        <Header>
            <EvaluationsSummary>
                <EvaluationDateRange strong>
                    {startDate} - {endDate}
                </EvaluationDateRange>
                <EvaluationResults>
                    <ResultTypeCount>
                        <Typography.Text
                            style={{ color: getResultColor(AssertionResultType.Success), fontWeight: 600 }}
                        >
                            {formatNumber(succeededCount)}
                        </Typography.Text>{' '}
                        passed
                    </ResultTypeCount>
                    <ResultTypeCount>
                        <Typography.Text
                            style={{ color: getResultColor(AssertionResultType.Failure), fontWeight: 600 }}
                        >
                            {formatNumber(failedCount)}
                        </Typography.Text>{' '}
                        failed
                    </ResultTypeCount>
                    {errorCount ? (
                        <ResultTypeCount>
                            <ErrorEvaluationsCount>{formatNumber(errorCount)}</ErrorEvaluationsCount> error
                            {errorCount > 1 ? 's' : ''}
                        </ResultTypeCount>
                    ) : null}
                </EvaluationResults>
            </EvaluationsSummary>
            <PrefixedSelect
                prefixText="Show "
                values={Object.values(LOOKBACK_WINDOWS).map((window) => window.text)}
                value={lookbackWindow.text}
                setValue={onChangeLookbackWindow}
            />
        </Header>
    );
};
