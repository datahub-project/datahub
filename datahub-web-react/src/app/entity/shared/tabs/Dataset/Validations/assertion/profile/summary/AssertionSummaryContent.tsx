import { Divider } from 'antd';
import React from 'react';

import { getNextScheduleEvaluationTimeMs } from '@app/entity/shared/tabs/Dataset/Validations/acrylUtils';
import { tryGetScheduleFromMonitor } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/shared/utils';
import { AssertionSummarySection } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummarySection';
import { AssertionResultsTable } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/result/table/AssertionResultsTable';
import { AssertionResultsTimeline } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/AssertionResultsTimeline';
import { AssertionScheduleSummary } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/schedule/AssertionScheduleSummary';

import { Assertion, Monitor, MonitorMode } from '@types';

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
