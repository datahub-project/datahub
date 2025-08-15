import React from 'react';

import { AssertionSummaryContent } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryContent';
import { AssertionSummaryLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryLoading';

import { Assertion } from '@types';

type Props = {
    loading: boolean;
    assertion?: Assertion;
};

export const AssertionSummaryTab = ({ loading, assertion }: Props) => {
    return (
        <>
            {loading || !assertion ? (
                <AssertionSummaryLoading />
            ) : (
                <AssertionSummaryContent assertion={assertion as Assertion} />
            )}
        </>
    );
};
