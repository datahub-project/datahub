/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Divider } from 'antd';
import React from 'react';

import { AssertionSummarySection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummarySection';
import { AssertionResultsTable } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/table/AssertionResultsTable';
import { AssertionResultsTimeline } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/AssertionResultsTimeline';
import { AssertionScheduleSummary } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/schedule/AssertionScheduleSummary';

import { Assertion } from '@types';

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
