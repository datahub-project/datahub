import React from 'react';

import { AssertionSummaryTab } from './summary/AssertionSummaryTab';
import { AssertionSettingsTab } from './settings/AssertionSettingsTab';
import { Assertion, DataContract, Entity, Monitor } from '../../../../../../../../types.generated';
import { useGetAssertionWithMonitorsQuery } from '../../../../../../../../graphql/monitor.generated';
import { AssertionProfileHeader } from './AssertionProfileHeader';
import { AssertionTabs } from './AssertionTabs';
import { AssertionProfileFooter } from './AssertionProfileFooter';
import { AssertionProfileHeaderLoading } from './AssertionProfileHeaderLoading';
import { AssertionEditabilityScopeType, getAssertionEditabilityType } from './summary/shared/assertionUtils';

enum TabType {
    Summary = 'Summary',
    Settings = 'Settings',
}

type Props = {
    urn: string;
    entity: Entity; // TODO: ideally this would be a field on the assertion itself.
    contract?: DataContract; // TODO: ideally this would be a field available on the assertion itself.
    // TODO: Ideally this would come from the load of the assertion details itself with the GraphQL call.
    // Currently this is a function of privileges on the target dataset!
    canEditAssertion: boolean;
    canEditMonitor: boolean;
    close: () => void;
    refetch?: () => void;
};

// TODO: Handling Loading Errors.

export const AssertionProfile = ({
    urn,
    entity,
    contract,
    canEditAssertion,
    canEditMonitor,
    close,
    refetch,
}: Props) => {
    const {
        data,
        loading,
        refetch: localRefetch,
    } = useGetAssertionWithMonitorsQuery({ variables: { assertionUrn: urn } });
    const assertion = data?.assertion as Assertion;
    const monitor = data?.assertion?.monitor?.relationships?.[0]?.entity as Monitor;
    const result = assertion?.runEvents?.runEvents[0]?.result;
    const editAllowed = canEditMonitor && canEditAssertion;

    const fullRefetch = () => {
        localRefetch();
        refetch?.();
    };

    const tabs = [
        {
            key: TabType.Summary,
            label: 'Summary',
            content: <AssertionSummaryTab loading={loading} assertion={assertion} monitor={monitor} />,
        },
        {
            key: TabType.Settings,
            label: 'Settings',
            content: (
                <AssertionSettingsTab
                    loading={loading}
                    assertion={assertion}
                    entity={entity}
                    refetch={fullRefetch}
                    monitor={monitor}
                    editable={
                        !!assertion && getAssertionEditabilityType(assertion) !== AssertionEditabilityScopeType.NONE
                    }
                    editAllowed={editAllowed}
                />
            ),
        },
    ];

    return (
        <>
            {(loading && <AssertionProfileHeaderLoading />) || (
                <AssertionProfileHeader
                    assertion={assertion}
                    monitor={monitor}
                    contract={contract}
                    result={result || undefined}
                    canEditAssertion={canEditAssertion}
                    canEditMonitor={canEditMonitor}
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
