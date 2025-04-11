import React from 'react';

import { Divider } from 'antd';

import { Assertion } from '../../../../../../../../../types.generated';
import { AssertionSummarySection } from './AssertionSummarySection';
import { AssertionResultsTimeline } from './result/timeline/AssertionResultsTimeline';
import { AssertionResultsTable } from './result/table/AssertionResultsTable';
import { AssertionScheduleSummary } from './schedule/AssertionScheduleSummary';

type Props = {
    assertion: Assertion;
};

export const AssertionSummaryContent = ({ assertion }: Props) => {
    const lastEvaluatedAtMillis = assertion.runEvents?.runEvents?.[0]?.timestampMillis;
    return (
        <>
            {/* NOTE: the timeline chart will have a title, so no need to add a section title here */}
            <AssertionSummarySection>
                <AssertionResultsTimeline assertion={assertion} />
            </AssertionSummarySection>
            <Divider />
            <AssertionSummarySection title="Activity">
                <AssertionResultsTable assertion={assertion} />
            </AssertionSummarySection>
            <Divider />
            <AssertionSummarySection title="Schedule details">
                <AssertionScheduleSummary assertion={assertion} lastEvaluatedAtMillis={lastEvaluatedAtMillis} />
            </AssertionSummarySection>
        </>
    );
};
