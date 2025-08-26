import React from 'react';

import { AssertionProfileFooter } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileFooter';
import { AssertionProfileHeader } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileHeader';
import { AssertionProfileHeaderLoading } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileHeaderLoading';
import { AssertionTabs } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/AssertionTabs';
import { AssertionSettingsTab } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/settings/AssertionSettingsTab';
import { AssertionSummaryTab } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryTab';
import {
    AssertionEditabilityScopeType,
    getAssertionEditabilityType,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/assertionUtils';

import { useGetAssertionWithMonitorsQuery } from '@graphql/monitor.generated';
import { Assertion, DataContract, Entity, Monitor } from '@types';

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
