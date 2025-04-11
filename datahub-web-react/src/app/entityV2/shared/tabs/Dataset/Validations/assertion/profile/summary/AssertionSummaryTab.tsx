import React from 'react';

import { Assertion } from '../../../../../../../../../types.generated';
import { AssertionSummaryLoading } from './AssertionSummaryLoading';
import { AssertionSummaryContent } from './AssertionSummaryContent';

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
