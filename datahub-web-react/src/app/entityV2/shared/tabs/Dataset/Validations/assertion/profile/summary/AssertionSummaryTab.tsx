import React from 'react';

import { AssertionSummaryContent } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryContent';
import { AssertionSummaryLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryLoading';

import { Assertion, Maybe, Monitor } from '@types';

type Props = {
    loading: boolean;
    assertion: Maybe<Assertion>;
    monitor?: Monitor;
    openAssertionNote: () => void;
    refreshData?: () => Promise<unknown>;
};

export const AssertionSummaryTab = ({ loading, assertion, monitor, openAssertionNote, refreshData }: Props) => {
    return (
        <>
            {loading || !assertion ? (
                <AssertionSummaryLoading />
            ) : (
                <AssertionSummaryContent
                    assertion={assertion}
                    monitor={monitor}
                    openAssertionNote={openAssertionNote}
                    refreshData={refreshData}
                />
            )}
        </>
    );
};
