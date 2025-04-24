import React from 'react';

import { AssertionSummaryContent } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryContent';
import { AssertionSummaryLoading } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryLoading';

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
