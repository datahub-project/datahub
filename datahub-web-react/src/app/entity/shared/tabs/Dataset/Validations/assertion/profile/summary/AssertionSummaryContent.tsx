import React from 'react';

import { Divider } from 'antd';

import { Assertion, Monitor, MonitorMode } from '../../../../../../../../../types.generated';
import { getNextScheduleEvaluationTimeMs } from '../../../acrylUtils';
import { AssertionSummarySection } from './AssertionSummarySection';
import { AssertionResultsTimeline } from './result/timeline/AssertionResultsTimeline';
import { AssertionResultsTable } from './result/table/AssertionResultsTable';
import { AssertionScheduleSummary } from './schedule/AssertionScheduleSummary';
import { tryGetScheduleFromMonitor } from '../shared/utils';

type Props = {
    assertion: Assertion;
    monitor?: Monitor;
};

export const AssertionSummaryContent = ({ assertion, monitor }: Props) => {
    const isStopped = monitor?.info?.status?.mode === MonitorMode.Inactive;
    const schedule = tryGetScheduleFromMonitor(monitor);
    const lastEvaluatedAtMillis = assertion.runEvents?.runEvents?.[0]?.timestampMillis;
    const nextEvaluatedAtMillis = schedule && getNextScheduleEvaluationTimeMs(schedule);
    return (
        <>
            {/* NOTE: the timeline chart will have a title, so no need to add a section title here */}
            <AssertionSummarySection>
                <AssertionResultsTimeline assertion={assertion} monitor={monitor} />
            </AssertionSummarySection>
            <Divider />
            <AssertionSummarySection title="Activity">
                <AssertionResultsTable assertion={assertion} />
            </AssertionSummarySection>
            <Divider />
            <AssertionSummarySection title="Schedule details">
                <AssertionScheduleSummary
                    assertion={assertion}
                    monitor={monitor}
                    schedule={schedule}
                    lastEvaluatedAtMillis={lastEvaluatedAtMillis}
                    nextEvaluatedAtMillis={nextEvaluatedAtMillis}
                    isStopped={isStopped}
                />
            </AssertionSummarySection>
        </>
    );
};
