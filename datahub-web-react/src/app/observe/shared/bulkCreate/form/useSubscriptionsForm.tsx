import { Select, Text } from '@components';
import { Alert, Divider, Switch, Tree } from 'antd';
import { DataNode } from 'antd/lib/tree';
import React, { Key, useState } from 'react';
import styled from 'styled-components';

import { SubscriptionsFormState } from '@app/observe/shared/bulkCreate/form/types';
import { getEntityChangeTypesFromCheckedKeys, getTreeDataForEntity } from '@app/shared/subscribe/drawer/utils';
import useGroupRelationships from '@app/shared/subscribe/useGroupRelationships';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useGetAuthenticatedUserUrn } from '@app/useGetAuthenticatedUser';

import { CorpGroup, EntityChangeDetailsInput, EntityChangeType, EntityRelationship, EntityType } from '@types';

// Default selected notification types
const DEFAULT_SELECTED_KEYS = [
    // Assertion changes
    EntityChangeType.AssertionFailed,
    EntityChangeType.AssertionPassed,
    EntityChangeType.AssertionError,
    // Incident changes
    EntityChangeType.IncidentRaised,
    EntityChangeType.IncidentResolved,
    // Schema changes
    EntityChangeType.OperationColumnAdded,
    EntityChangeType.OperationColumnRemoved,
    EntityChangeType.OperationColumnModified,
];

const SectionContainer = styled.div({
    marginTop: 16,
    marginBottom: 16,
});

const SectionTitle = styled(Text)({
    fontSize: 14,
    fontWeight: 600,
    marginBottom: 8,
    display: 'block',
});

const SwitchContainer = styled.div({
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    marginBottom: 12,
});

const TreeContainer = styled.div({
    marginLeft: 16,
    marginTop: 8,
    '.ant-tree-checkbox .ant-tree-checkbox-inner': {
        borderColor: '#d9d9d9',
    },
    '.ant-tree-node-content-wrapper': {
        background: 'none',
        cursor: 'auto',
        '&:hover': {
            background: 'none',
        },
    },
    '.ant-tree .ant-tree-node-content-wrapper.ant-tree-node-selected': {
        backgroundColor: 'transparent',
    },
});

const GroupSelectContainer = styled.div({
    marginTop: 8,
    marginLeft: 16,
});

const StyledAlert = styled(Alert)({
    margin: '16px 0',
    '.anticon': {
        fontSize: '16px',
    },
});

export const useSubscriptionsForm = (): {
    component: React.ReactNode;
    state: SubscriptionsFormState;
} => {
    // --------------------------------- State variables --------------------------------- //
    const entityRegistry = useEntityRegistry();
    const { relationships } = useGroupRelationships();
    const currentUserUrn = useGetAuthenticatedUserUrn();

    // Personal subscription state
    const [personalSubscriptionEnabled, setPersonalSubscriptionEnabled] = useState<boolean>(false);
    const [personalCheckedKeys, setPersonalCheckedKeys] = useState<Key[]>([]);

    // Group subscription state
    const [groupSubscriptionEnabled, setGroupSubscriptionEnabled] = useState<boolean>(false);
    const [selectedGroups, setSelectedGroups] = useState<string[]>([]);
    const [groupCheckedKeys, setGroupCheckedKeys] = useState<Key[]>([]);

    // --------------------------------- Helper functions --------------------------------- //
    const convertKeysToEntityChangeTypes = (keys: Key[]): EntityChangeDetailsInput[] => {
        const entityChangeTypes = getEntityChangeTypesFromCheckedKeys(keys);
        return entityChangeTypes.map((type) => ({
            entityChangeType: type,
        }));
    };

    const convertGroupRelationshipToOption = (relationship: EntityRelationship) => {
        const group: CorpGroup = relationship?.entity as CorpGroup;
        return {
            label: entityRegistry.getDisplayName(EntityType.CorpGroup, group),
            value: group?.urn,
        };
    };

    const groupOptions =
        relationships
            ?.filter((relationship) => !!relationship)
            .map((relationship) => convertGroupRelationshipToOption(relationship as EntityRelationship)) || [];

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

    // Get tree data for entity change types (assuming Dataset for now)
    const treeData: DataNode[] = getTreeDataForEntity(EntityType.Dataset);

    // --------------------------------- Render UI --------------------------------- //
    const component = (
        <div>
            <Text size="md" color="gray" colorLevel={1700}>
                Subscribe to changes on the selected datasets to get notified when they occur.
            </Text>

            <StyledAlert
                message="Your notification preferences will be added to existing subscriptions"
                description="Selecting notification types below will add new subscriptions without affecting any notifications you're already receiving. Your current subscriptions will remain active."
                type="info"
                showIcon
            />

            {/* --------------------------------- Personal Subscription Section --------------------------------- */}
            <SectionContainer>
                <SwitchContainer>
                    <Switch
                        checked={personalSubscriptionEnabled}
                        onChange={(checked) => {
                            setPersonalSubscriptionEnabled(checked);
                            if (checked) {
                                setPersonalCheckedKeys(DEFAULT_SELECTED_KEYS);
                            } else {
                                setPersonalCheckedKeys([]);
                            }
                        }}
                    />
                    <SectionTitle>Subscribe me</SectionTitle>
                </SwitchContainer>

                {personalSubscriptionEnabled && (
                    <TreeContainer>
                        <Tree
                            checkable
                            checkedKeys={personalCheckedKeys}
                            onCheck={onPersonalCheck}
                            treeData={treeData}
                            defaultExpandAll
                        />
                    </TreeContainer>
                )}
            </SectionContainer>

            <Divider style={{ margin: '16px 0' }} />

            {/* --------------------------------- Group Subscription Section --------------------------------- */}
            <SectionContainer>
                <SwitchContainer>
                    <Switch
                        checked={groupSubscriptionEnabled}
                        onChange={(checked) => {
                            setGroupSubscriptionEnabled(checked);
                            if (checked) {
                                setGroupCheckedKeys(DEFAULT_SELECTED_KEYS);
                            } else {
                                setSelectedGroups([]);
                                setGroupCheckedKeys([]);
                            }
                        }}
                    />
                    <SectionTitle>Subscribe groups</SectionTitle>
                </SwitchContainer>

                {groupSubscriptionEnabled && [
                    <GroupSelectContainer key="group-select">
                        <Select
                            isMultiSelect
                            placeholder="Select groups to notify..."
                            values={selectedGroups}
                            onUpdate={onUpdateGroups}
                            options={groupOptions}
                            width="full"
                            showClear
                        />
                    </GroupSelectContainer>,

                    selectedGroups.length > 0 && (
                        <TreeContainer key="group-tree">
                            <Tree
                                checkable
                                checkedKeys={groupCheckedKeys}
                                onCheck={onGroupCheck}
                                treeData={treeData}
                                defaultExpandAll
                            />
                        </TreeContainer>
                    ),
                ]}
            </SectionContainer>
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
