import React from 'react';

import { AssertionSummaryContent } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryContent';
import { AssertionSummaryLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryLoading';
import { Assertion, Maybe } from '@src/types.generated';

type Props = {
    loading: boolean;
    assertion?: Maybe<Assertion>;
};

export const AssertionSummaryTab = ({ loading, assertion }: Props) => {
    return (
        <>{loading || !assertion ? <AssertionSummaryLoading /> : <AssertionSummaryContent assertion={assertion} />}</>
    );
};
