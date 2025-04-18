import React from 'react';

import { Assertion, Monitor } from '../../../../../../../../../types.generated';
import { AssertionSummaryLoading } from './AssertionSummaryLoading';
import { AssertionSummaryContent } from './AssertionSummaryContent';

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
