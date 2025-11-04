import { Divider } from 'antd';
import React from 'react';

import { getNextScheduleEvaluationTimeMs } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { tryGetScheduleFromMonitor } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/utils';
import { AssertionSummarySection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummarySection';
import { AssertionResultsTable } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/table/AssertionResultsTable';
import { AssertionResultsTimeline } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/AssertionResultsTimeline';
import { AssertionScheduleSummary } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/schedule/AssertionScheduleSummary';

import { Assertion, Maybe, Monitor, MonitorMode } from '@types';

type Props = {
    assertion: Assertion;
    monitor?: Maybe<Monitor>;
    openAssertionNote: () => void;
    refreshData?: () => Promise<unknown>;
};

export const AssertionSummaryContent = ({ assertion, monitor, openAssertionNote, refreshData }: Props) => {
    const isStopped = monitor?.info?.status?.mode === MonitorMode.Inactive;
    const schedule = tryGetScheduleFromMonitor(monitor);
    const lastEvaluatedAtMillis = assertion.runEvents?.runEvents?.[0]?.timestampMillis;
    const nextEvaluatedAtMillis = schedule && getNextScheduleEvaluationTimeMs(schedule);
    return (
        <>
            {/* NOTE: the timeline chart will have a title, so no need to add a section title here */}
            <AssertionSummarySection>
                <AssertionResultsTimeline
                    assertion={assertion}
                    monitor={monitor}
                    openAssertionNote={openAssertionNote}
                    refreshData={refreshData}
                />
            </AssertionSummarySection>
            <Divider />
            <AssertionSummarySection title="Activity">
                <AssertionResultsTable assertion={assertion} monitor={monitor} openAssertionNote={openAssertionNote} />
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
