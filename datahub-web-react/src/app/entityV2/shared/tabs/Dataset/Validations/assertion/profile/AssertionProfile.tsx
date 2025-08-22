import React, { useEffect, useRef, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { AssertionProfileFooter } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileFooter';
import { AssertionProfileHeader } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileHeader';
import { AssertionProfileHeaderLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileHeaderLoading';
import { AssertionTabs } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionTabs';
import { AssertionNoteTab } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/note/AssertionNoteTab';
import { AssertionSettingsTab } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/settings/AssertionSettingsTab';
import { AssertionSummaryTab } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryTab';
import {
    AssertionEditabilityScopeType,
    getAssertionEditabilityType,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/assertionUtils';

import { useGetAssertionWithMonitorsQuery } from '@graphql/monitor.generated';
import { Assertion, DataContract, Entity, Maybe, Monitor } from '@types';

enum TabType {
    Summary = 'Summary',
    Note = 'Note',
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

    const fullRefetch = async () => {
        try {
            await localRefetch();
        } catch (error) {
            console.error('Error refetching assertion details:', error);
        }
        refetch?.();
    };

    // TODO: we should move these casts to a deep partial assertion type
    const assertion = data?.assertion as Maybe<Assertion>;
    const monitor = data?.assertion?.monitor?.relationships?.[0]?.entity as Monitor;
    const result = assertion?.runEvents?.runEvents[0]?.result;
    const editAllowed = canEditMonitor && canEditAssertion;

    const [selectedTab, setSelectedTab] = useState<string>(TabType.Summary);
    const hasViewedNoteTabRef = useRef(false);

    const openAssertionNote = () => {
        setSelectedTab(TabType.Note);
    };

    // Refresh assertion details when switching to the settings tab
    // This is because the assertion settings (such as exclusion window) may have changed by the other tabs
    const [isRefetchingForSettingsTab, setIsRefetchingForSettingsTab] = useState(false);
    useEffect(() => {
        if (selectedTab === TabType.Settings) {
            setIsRefetchingForSettingsTab(true);
            fullRefetch().finally(() => {
                setIsRefetchingForSettingsTab(false);
            });
        }
        if (selectedTab === TabType.Note) {
            if (!hasViewedNoteTabRef.current) {
                hasViewedNoteTabRef.current = true;
                analytics.event({
                    type: EventType.ViewAssertionNoteTabEvent,
                    assertionUrn: assertion?.urn || '',
                    entityUrn: entity?.urn || '',
                    assertionType: assertion?.info?.type || '',
                });
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedTab]);

    const tabs = [
        {
            key: TabType.Summary,
            label: 'Summary',
            content: (
                <AssertionSummaryTab
                    loading={loading}
                    assertion={assertion}
                    monitor={monitor}
                    openAssertionNote={openAssertionNote}
                    refreshData={fullRefetch}
                />
            ),
        },
        {
            key: TabType.Note,
            label: 'Notes',
            content: <AssertionNoteTab loading={loading} assertion={assertion} editAllowed={editAllowed} />,
        },
        {
            key: TabType.Settings,
            label: 'Settings',
            content: (
                <AssertionSettingsTab
                    loading={loading || isRefetchingForSettingsTab}
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
            {loading || !assertion ? (
                <AssertionProfileHeaderLoading />
            ) : (
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
            <AssertionTabs selectedTab={selectedTab} setSelectedTab={setSelectedTab} tabs={tabs} />
            <AssertionProfileFooter />
        </>
    );
};
