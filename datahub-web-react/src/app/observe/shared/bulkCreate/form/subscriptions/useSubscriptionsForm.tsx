import { Button, SimpleSelect, Text, colors } from '@components';
import { Question } from '@phosphor-icons/react';
import { Switch, Tooltip } from 'antd';
import { DataNode } from 'antd/lib/tree';
import React, { Key, useState } from 'react';
import styled from 'styled-components';

import CreateGroupModal from '@app/identity/group/CreateGroupModal';
import { NotificationTypesSelector } from '@app/observe/shared/bulkCreate/form/subscriptions/NotificationTypesSelector';
import { SubscriptionsFormState } from '@app/observe/shared/bulkCreate/form/types';
import Loading from '@app/shared/Loading';
import { getGroupOptions } from '@app/shared/subscribe/drawer/section/SelectGroupSection.utils';
import {
    DEFAULT_SELECTED_KEYS,
    getEntityChangeTypesFromCheckedKeys,
    getTreeDataForEntity,
} from '@app/shared/subscribe/drawer/utils';
import useGroupRelationships from '@app/shared/subscribe/useGroupRelationships';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useGetAuthenticatedUserUrn } from '@app/useGetAuthenticatedUser';

import { CorpGroup, EntityChangeDetailsInput, EntityRelationshipsResult, EntityType } from '@types';

const SectionContainer = styled.div`
    margin-top: 32px;
    margin-bottom: 32px;
`;

const SectionTitle = styled(Text)`
    font-size: 14px;
    font-weight: 600;
    margin-bottom: 0;
    display: block;
    line-height: 1.5;
    user-select: none;
`;

const SwitchContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 12px;
    cursor: pointer;

    &:hover {
        opacity: 0.8;
    }
`;

const GroupSelectContainer = styled.div`
    margin-top: 16px;
    margin-bottom: 24px;
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const SubtitleContainer = styled.div`
    margin-top: 0;
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    gap: 6px;
`;

const SubtitleText = styled(Text)`
    font-size: 13px;
    color: ${colors.gray[600]};
    margin-bottom: 0;
`;

const TooltipIcon = styled.div`
    cursor: pointer;
    display: flex;
    align-items: center;

    svg {
        height: 14px;
        width: 14px;
        color: ${colors.gray[400]};
    }

    &:hover svg {
        color: ${colors.gray[600]};
    }
`;

const NewGroupButton = styled(Button)`
    align-self: flex-end;
`;

export const useSubscriptionsForm = (): {
    component: React.ReactNode;
    state: SubscriptionsFormState;
} => {
    // --------------------------------- State variables --------------------------------- //
    const entityRegistry = useEntityRegistry();
    const { relationships, ownedGroupSearchResults, refetch: refetchGroups } = useGroupRelationships();
    const currentUserUrn = useGetAuthenticatedUserUrn();

    // Personal subscription state
    const [personalSubscriptionEnabled, setPersonalSubscriptionEnabled] = useState<boolean>(false);
    const [personalCheckedKeys, setPersonalCheckedKeys] = useState<Key[]>([]);
    const [personalNotificationTypesExpanded, setPersonalNotificationTypesExpanded] = useState<boolean>(false);

    // Group subscription state
    const [groupSubscriptionEnabled, setGroupSubscriptionEnabled] = useState<boolean>(false);
    const [selectedGroups, setSelectedGroups] = useState<string[]>([]);
    const [groupCheckedKeys, setGroupCheckedKeys] = useState<Key[]>([]);
    const [groupNotificationTypesExpanded, setGroupNotificationTypesExpanded] = useState<boolean>(false);

    const [isCreateGroupModalOpen, setIsCreateGroupModalOpen] = useState(false);
    const [isRefetching, setIsRefetching] = useState(false);

    // --------------------------------- Helper functions --------------------------------- //
    const convertKeysToEntityChangeTypes = (keys: Key[]): EntityChangeDetailsInput[] => {
        const entityChangeTypes = getEntityChangeTypesFromCheckedKeys(keys);
        return entityChangeTypes.map((type) => ({
            entityChangeType: type,
        }));
    };

    const groupOptions = getGroupOptions(
        relationships as EntityRelationshipsResult['relationships'],
        ownedGroupSearchResults,
        entityRegistry,
    );

    // --------------------------------- Event handlers --------------------------------- //
    const onPersonalCheck = (checkedKeysValue: any) => {
        setPersonalCheckedKeys(checkedKeysValue);
    };

    const onGroupCheck = (checkedKeysValue: any) => {
        setGroupCheckedKeys(checkedKeysValue);
    };

    const onUpdateGroups = (newSelectedGroups: string[] | undefined) => {
        setSelectedGroups(newSelectedGroups || []);
    };

    const handlePersonalSwitchToggle = () => {
        const newValue = !personalSubscriptionEnabled;
        setPersonalSubscriptionEnabled(newValue);
        setPersonalNotificationTypesExpanded(false); // Reset collapse state
        if (newValue) {
            setPersonalCheckedKeys(DEFAULT_SELECTED_KEYS);
        } else {
            setPersonalCheckedKeys([]);
        }
    };

    const handleGroupSwitchToggle = () => {
        const newValue = !groupSubscriptionEnabled;
        setGroupSubscriptionEnabled(newValue);
        setGroupNotificationTypesExpanded(false); // Reset collapse state
        if (newValue) {
            setGroupCheckedKeys(DEFAULT_SELECTED_KEYS);
        } else {
            setSelectedGroups([]);
            setGroupCheckedKeys([]);
        }
    };

    // Get tree data for entity change types (assuming Dataset for now)
    const treeData: DataNode[] = getTreeDataForEntity(EntityType.Dataset);

    // --------------------------------- Render UI --------------------------------- //
    const component = (
        <div>
            <Text size="lg" color="gray" colorLevel={600} weight="semiBold">
                Get notified for Schema changes, Assertions, Incidents, and more.
            </Text>

            {/* --------------------------------- Subscription behavior note --------------------------------- */}
            <SubtitleContainer>
                <SubtitleText>Notification types will be added to any existing subscriptions.</SubtitleText>
                <Tooltip title="If a dataset already has notification subscriptions set up, we'll add your selected alert types to the existing subscription instead of creating a new one. This prevents duplicate notifications and keeps all alerts organized in a single subscription.">
                    <TooltipIcon>
                        <Question />
                    </TooltipIcon>
                </Tooltip>
            </SubtitleContainer>

            {/* --------------------------------- Personal Subscription Section --------------------------------- */}
            <SectionContainer>
                <SwitchContainer onClick={handlePersonalSwitchToggle}>
                    <Switch checked={personalSubscriptionEnabled} onChange={handlePersonalSwitchToggle} />
                    <SectionTitle>Subscribe me</SectionTitle>
                </SwitchContainer>

                {personalSubscriptionEnabled && (
                    <NotificationTypesSelector
                        checkedKeys={personalCheckedKeys}
                        onCheck={onPersonalCheck}
                        notificationTypesExpanded={personalNotificationTypesExpanded}
                        setNotificationTypesExpanded={setPersonalNotificationTypesExpanded}
                        treeData={treeData}
                        isPersonal
                        groupUrn={undefined}
                        panelKey="personal-notifications"
                    />
                )}
            </SectionContainer>

            {/* --------------------------------- Group Subscription Section --------------------------------- */}
            <SectionContainer>
                <SwitchContainer onClick={handleGroupSwitchToggle}>
                    <Switch checked={groupSubscriptionEnabled} onChange={handleGroupSwitchToggle} />
                    <SectionTitle>Subscribe group</SectionTitle>
                </SwitchContainer>

                {groupSubscriptionEnabled && (
                    <>
                        <GroupSelectContainer>
                            {/* Select group dropdown */}
                            <SimpleSelect
                                isMultiSelect={false}
                                placeholder="Select groups to notify..."
                                values={selectedGroups}
                                onUpdate={onUpdateGroups}
                                options={groupOptions}
                                width="full"
                                showClear
                            />
                            {/* Option to create a group instead */}
                            <NewGroupButton
                                variant="link"
                                onClick={() => setIsCreateGroupModalOpen(true)}
                                disabled={isRefetching}
                            >
                                {isRefetching ? (
                                    [<Loading marginTop={0} height={12} />, <Text>Creating...</Text>]
                                ) : (
                                    <Text>Create Group</Text>
                                )}
                            </NewGroupButton>
                        </GroupSelectContainer>

                        {selectedGroups.length > 0 && (
                            <NotificationTypesSelector
                                checkedKeys={groupCheckedKeys}
                                onCheck={onGroupCheck}
                                notificationTypesExpanded={groupNotificationTypesExpanded}
                                setNotificationTypesExpanded={setGroupNotificationTypesExpanded}
                                treeData={treeData}
                                isPersonal={false}
                                groupUrn={selectedGroups[0]}
                                panelKey="group-notifications"
                            />
                        )}
                    </>
                )}
            </SectionContainer>

            {/* --------------------------------- Create group modal --------------------------------- */}
            {isCreateGroupModalOpen && (
                <CreateGroupModal
                    onClose={() => setIsCreateGroupModalOpen(false)}
                    onCreate={(group: CorpGroup) => {
                        setIsCreateGroupModalOpen(false);
                        setIsRefetching(true);
                        setTimeout(() => {
                            refetchGroups();
                            setIsRefetching(false);
                            setSelectedGroups([group.urn]);
                        }, 3000);
                    }}
                />
            )}
        </div>
    );

    return {
        component,
        state: {
            personalSubscriptionEnabled,
            personalEntityChangeTypes: convertKeysToEntityChangeTypes(personalCheckedKeys),
            personalUserUrn: currentUserUrn,
            groupSubscriptionEnabled,
            selectedGroups,
            groupEntityChangeTypes: convertKeysToEntityChangeTypes(groupCheckedKeys),
        },
    };
};
