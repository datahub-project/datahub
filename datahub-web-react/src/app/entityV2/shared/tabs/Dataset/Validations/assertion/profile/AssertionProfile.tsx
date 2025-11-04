import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { getEntityUrnForAssertion, getSiblingWithUrn } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
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

import { useGetDatasetContractQuery } from '@graphql/contract.generated';
import { useGetAssertionWithMonitorsQuery } from '@graphql/monitor.generated';
import { Assertion, AssertionType, DataContract, Maybe, Monitor } from '@types';

enum TabType {
    Summary = 'Summary',
    Note = 'Note',
    Settings = 'Settings',
}

type Props = {
    urn: string;
    entity: GenericEntityProperties; // TODO: ideally this would be a field on the assertion itself.
    // TODO: Ideally this would come from the load of the assertion details itself with the GraphQL call.
    // Currently this is a function of privileges on the target dataset!
    canEditAssertions: boolean;
    canEditMonitors: boolean;
    canEditSqlAssertions: boolean;
    close: () => void;
};

// TODO: Handling Loading Errors.

export const AssertionProfile = ({
    urn,
    entity,
    canEditAssertions,
    canEditMonitors,
    canEditSqlAssertions,
    close,
}: Props) => {
    const [selectedTab, setSelectedTab] = useState<string>(TabType.Summary);
    const openAssertionNote = useCallback(() => {
        setSelectedTab(TabType.Note);
    }, []);
    // Refresh assertion details when switching to the settings tab
    // This is because the assertion settings (such as exclusion window) may have changed by the other tabs
    const [isRefetchingForSettingsTab, setIsRefetchingForSettingsTab] = useState(false);
    const hasViewedNoteTabRef = useRef(false);
    const {
        data: assertionData,
        loading: assertionLoading,
        refetch: assertionRefetch,
    } = useGetAssertionWithMonitorsQuery({ variables: { assertionUrn: urn }, fetchPolicy: 'cache-first' });

    const assertion = assertionData?.assertion as Maybe<Assertion>;
    const monitor = assertionData?.assertion?.monitor?.relationships?.[0]?.entity as Maybe<Monitor>;
    const result = assertion?.runEvents?.runEvents[0]?.result;
    const assertionEntityUrn = assertion ? getEntityUrnForAssertion(assertion) : undefined;
    const canEditAssertion = assertion
        ? (assertion?.info?.type === AssertionType.Sql && canEditSqlAssertions) || canEditAssertions
        : false;
    const editAllowed = canEditMonitors && canEditAssertion;
    const assertionEntitySibling = assertionEntityUrn ? getSiblingWithUrn(entity, assertionEntityUrn) : undefined;

    const {
        data: contractData,
        refetch: contractRefetch,
        loading: contractLoading,
    } = useGetDatasetContractQuery({
        // Query will be skipped if assertionEntityUrn is undefined
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        variables: { urn: assertionEntityUrn! },
        fetchPolicy: 'cache-first',
        skip: !assertionEntityUrn,
    });

    const fullRefetch = useCallback(async () => {
        try {
            await Promise.allSettled([assertionRefetch(), contractRefetch()]);
        } catch (error) {
            console.error('Error refetching assertion details:', error);
        }
    }, [assertionRefetch, contractRefetch]);

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
                    entityUrn: assertionEntityUrn || '',
                    assertionType: assertion?.info?.type || '',
                });
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedTab]);

    const contract = contractData?.dataset?.contract as DataContract | null | undefined;
    const isLoading = assertionLoading || contractLoading;

    const tabs = useMemo(
        () => [
            {
                key: TabType.Summary,
                label: 'Summary',
                content: (
                    <AssertionSummaryTab
                        loading={isLoading}
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
                content: <AssertionNoteTab loading={isLoading} assertion={assertion} editAllowed={editAllowed} />,
            },
            {
                key: TabType.Settings,
                label: 'Settings',
                content: (
                    <AssertionSettingsTab
                        loading={isLoading || isRefetchingForSettingsTab}
                        assertion={assertion}
                        entity={assertionEntitySibling}
                        refetch={fullRefetch}
                        monitor={monitor}
                        editable={
                            !!assertion && getAssertionEditabilityType(assertion) !== AssertionEditabilityScopeType.NONE
                        }
                        editAllowed={editAllowed}
                    />
                ),
            },
        ],
        [
            isLoading,
            assertion,
            monitor,
            openAssertionNote,
            fullRefetch,
            editAllowed,
            isRefetchingForSettingsTab,
            assertionEntitySibling,
        ],
    );

    return (
        <>
            {isLoading || !assertion ? (
                <AssertionProfileHeaderLoading />
            ) : (
                <AssertionProfileHeader
                    assertion={assertion}
                    monitor={monitor}
                    contract={contract}
                    result={result || undefined}
                    canEditAssertion={canEditAssertion}
                    canEditMonitor={canEditMonitors}
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
