import { Skeleton } from 'antd';
import React, { useCallback, useEffect, useRef, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { getSiblingWithUrn } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionSettingsTab } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/AssertionSettingsTab';
import { AssertionProfileFooter } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileFooter';
import { AssertionProfileHeader } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileHeader';
import { AssertionProfileHeaderLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileHeaderLoading';
import { AssertionTabs } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionTabs';
import { AssertionNoteTab } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/note/AssertionNoteTab';
import { AssertionSettingsLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/settings/AssertionSettingsLoading';
import { AssertionSummaryLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionSummaryLoading';
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
    const assertionEntityUrn = assertion?.info?.entityUrn;

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

    const fullRefetch = async () => {
        try {
            await Promise.allSettled([assertionRefetch(), contractRefetch()]);
        } catch (error) {
            console.error('Error refetching assertion details:', error);
        }
    };

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
    }, [selectedTab, assertion?.urn]);

    const contract = contractData?.dataset?.contract as DataContract | null | undefined;
    const isLoading = assertionLoading || contractLoading || isRefetchingForSettingsTab;

    const loadingTabs = [
        {
            key: TabType.Summary,
            label: 'Summary',
            content: <AssertionSummaryLoading />,
        },
        {
            key: TabType.Note,
            label: 'Notes',
            content: <Skeleton />,
        },
        {
            key: TabType.Settings,
            label: 'Settings',
            content: <AssertionSettingsLoading />,
        },
    ];

    if (isLoading || !assertion) {
        return (
            <>
                <AssertionProfileHeaderLoading />
                <AssertionTabs selectedTab={selectedTab} setSelectedTab={setSelectedTab} tabs={loadingTabs} />
                <AssertionProfileFooter />
            </>
        );
    }

    const monitor = assertionData?.assertion?.monitor?.relationships?.[0]?.entity as Maybe<Monitor>;
    const result = assertion?.runEvents?.runEvents[0]?.result;
    const canEditAssertion = (assertion.info?.type === AssertionType.Sql && canEditSqlAssertions) || canEditAssertions;
    const editAllowed = canEditMonitors && canEditAssertion;

    if (!assertionEntityUrn) {
        throw new Error(`Assertion entity urn not found for assertion ${assertion?.urn}`);
    }

    const assertionEntitySibling = getSiblingWithUrn(entity, assertionEntityUrn);
    if (!assertionEntitySibling) {
        throw new Error(`Assertion entity sibling not found for assertion ${assertion?.urn} on entity ${entity.urn}`);
    }

    const tabs = [
        {
            key: TabType.Summary,
            label: 'Summary',
            content: (
                <AssertionSummaryTab
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
            content: <AssertionNoteTab assertion={assertion} editAllowed={editAllowed} />,
        },
        {
            key: TabType.Settings,
            label: 'Settings',
            content: (
                <AssertionSettingsTab
                    assertion={assertion}
                    entity={assertionEntitySibling}
                    refetch={fullRefetch}
                    monitor={monitor}
                    editable={getAssertionEditabilityType(assertion) !== AssertionEditabilityScopeType.NONE}
                    editAllowed={editAllowed}
                />
            ),
        },
    ];

    return (
        <>
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
            <AssertionTabs selectedTab={selectedTab} setSelectedTab={setSelectedTab} tabs={tabs} />
            <AssertionProfileFooter />
        </>
    );
};
