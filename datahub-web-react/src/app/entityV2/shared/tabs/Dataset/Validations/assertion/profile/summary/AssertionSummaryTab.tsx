import React from 'react';

import { AssertionSummaryContent } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryContent';
import { AssertionSummaryLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryLoading';

import { Assertion, Monitor } from '@types';

type Props = {
    loading: boolean;
    assertion?: Assertion;
    monitor?: Monitor;
};

export const AssertionSummaryTab = ({ loading, assertion, monitor }: Props) => {
    return (
        <>
            {loading || !assertion ? (
                <AssertionSummaryLoading />
            ) : (
                <AssertionSummaryContent assertion={assertion as Assertion} monitor={monitor} />
            )}
        </>
    );
};
