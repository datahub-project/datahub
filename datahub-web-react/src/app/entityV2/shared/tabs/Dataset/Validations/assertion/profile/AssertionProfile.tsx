import React from 'react';

import { useGetAssertionWithRunEventsQuery } from '@src/graphql/assertion.generated';
import { AssertionSummaryTab } from './summary/AssertionSummaryTab';
import { Assertion, DataContract } from '../../../../../../../../types.generated';
import { AssertionProfileHeader } from './AssertionProfileHeader';
import { AssertionTabs } from './AssertionTabs';
import { AssertionProfileFooter } from './AssertionProfileFooter';
import { AssertionProfileHeaderLoading } from './AssertionProfileHeaderLoading';

enum TabType {
    Summary = 'Summary',
    Settings = 'Settings',
}

type Props = {
    urn: string;
    contract?: DataContract; // TODO: ideally this would be a field available on the assertion itself.
    close: () => void;
    refetch?: () => void;
};

// TODO: Handling Loading Errors.

export const AssertionProfile = ({ urn, contract, close, refetch }: Props) => {
    const {
        data,
        loading,
        refetch: localRefetch,
    } = useGetAssertionWithRunEventsQuery({ variables: { assertionUrn: urn } });
    const assertion = data?.assertion as Assertion;
    const result = assertion?.runEvents?.runEvents[0]?.result;

    const fullRefetch = () => {
        localRefetch();
        refetch?.();
    };

    const tabs = [
        {
            key: TabType.Summary,
            label: 'Summary',
            content: <AssertionSummaryTab loading={loading} assertion={assertion} />,
        },
    ];

    return (
        <>
            {(loading && <AssertionProfileHeaderLoading />) || (
                <AssertionProfileHeader
                    assertion={assertion}
                    contract={contract}
                    result={result || undefined}
                    canEditContract
                    refetch={fullRefetch}
                    close={close}
                />
            )}
            <AssertionTabs defaultSelectedTab={TabType.Summary} tabs={tabs} />
            <AssertionProfileFooter />
        </>
    );
};
