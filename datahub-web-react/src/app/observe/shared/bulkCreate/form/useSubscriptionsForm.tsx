import { SimpleSelect, Text, colors } from '@components';
import { CaretDown, CaretRight, Question } from '@phosphor-icons/react';
import { Collapse, Switch, Tooltip, Tree } from 'antd';
import { DataNode } from 'antd/lib/tree';
import { Check } from 'phosphor-react';
import React, { Key, useState } from 'react';
import styled from 'styled-components';

import { NotificationSettingsSection } from '@app/observe/shared/bulkCreate/form/notificationsConfiguration/NotificationSettingsSection';
import { SubscriptionsFormState } from '@app/observe/shared/bulkCreate/form/types';
import { generateSummaryText } from '@app/observe/shared/bulkCreate/form/useSubscriptionsForm.utils';
import { getEntityChangeTypesFromCheckedKeys, getTreeDataForEntity } from '@app/shared/subscribe/drawer/utils';
import useGroupRelationships from '@app/shared/subscribe/useGroupRelationships';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useGetAuthenticatedUserUrn } from '@app/useGetAuthenticatedUser';

import { CorpGroup, EntityChangeDetailsInput, EntityChangeType, EntityRelationship, EntityType } from '@types';

// Default selected notification types
const DEFAULT_SELECTED_KEYS = [
    // Assertion changes
    EntityChangeType.AssertionFailed,
    EntityChangeType.AssertionError,
    // Incident changes
    EntityChangeType.IncidentRaised,
    EntityChangeType.IncidentResolved,
    // Schema changes
    EntityChangeType.OperationColumnAdded,
    EntityChangeType.OperationColumnRemoved,
    EntityChangeType.OperationColumnModified,
];

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

const TreeContainer = styled.div`
    margin-left: 16px;
    margin-top: 8px;

    .ant-tree-checkbox .ant-tree-checkbox-inner {
        border-color: ${colors.gray[300]};
    }

    .ant-tree-node-content-wrapper {
        background: none;
        cursor: auto;

        &:hover {
            background: none;
        }
    }

    .ant-tree .ant-tree-node-content-wrapper.ant-tree-node-selected {
        background-color: transparent;
    }
`;

const GroupSelectContainer = styled.div`
    margin-top: 16px;
    margin-bottom: 24px;
`;

const StyledCollapse = styled(Collapse)`
    .ant-collapse-header {
        padding: 8px 0 !important;
        padding-bottom: 0px !important;
        align-items: center;
    }

    .ant-collapse-content-box {
        padding-top: 0px !important;
        padding-bottom: 0px !important;
    }

    .ant-collapse-arrow {
        margin-right: 8px !important;
        line-height: 32px;
    }

    &.ant-collapse {
        border-radius: 0 !important;
        border: none;
        background: none;
    }

    .ant-collapse-item {
        border-radius: 0 !important;
        border: none;

        &:last-child {
            border-bottom: none;
        }
    }
`;

const CollapseHeader = styled.div`
    display: flex;
    align-items: center;
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[500]};
`;

const StyledIcon = styled.div`
    svg {
        height: 16px;
        width: 16px;
        color: ${colors.gray[500]};
    }
`;

const SummaryLabel = styled(Text)`
    margin-left: 0;
    font-size: 14px;
    color: ${colors.gray[600]};
    font-weight: 600;
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

const SummaryLabelCheck = styled(Check)`
    position: relative;
    top: 2px;
    margin-right: 4px;
`;

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
    const [personalNotificationTypesExpanded, setPersonalNotificationTypesExpanded] = useState<boolean>(false);

    // Group subscription state
    const [groupSubscriptionEnabled, setGroupSubscriptionEnabled] = useState<boolean>(false);
    const [selectedGroups, setSelectedGroups] = useState<string[]>([]);
    const [groupCheckedKeys, setGroupCheckedKeys] = useState<Key[]>([]);
    const [groupNotificationTypesExpanded, setGroupNotificationTypesExpanded] = useState<boolean>(false);

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
                    <div style={{ marginTop: 16 }}>
                        {!personalNotificationTypesExpanded && personalCheckedKeys.length > 0 && (
                            <SummaryLabel>
                                {personalCheckedKeys.length ? <SummaryLabelCheck size={16} /> : null}{' '}
                                {generateSummaryText(personalCheckedKeys)}
                            </SummaryLabel>
                        )}
                        <StyledCollapse
                            ghost
                            expandIcon={({ isActive }) => (
                                <StyledIcon>{isActive ? <CaretDown /> : <CaretRight />}</StyledIcon>
                            )}
                            onChange={(activeKey) => {
                                setPersonalNotificationTypesExpanded(
                                    Array.isArray(activeKey) ? activeKey.length > 0 : !!activeKey,
                                );
                            }}
                        >
                            <Collapse.Panel
                                header={<CollapseHeader>Select Notification Types</CollapseHeader>}
                                key="personal-notifications"
                            >
                                <TreeContainer>
                                    <Tree
                                        checkable
                                        checkedKeys={personalCheckedKeys}
                                        onCheck={onPersonalCheck}
                                        treeData={treeData}
                                        defaultExpandAll
                                    />
                                </TreeContainer>
                            </Collapse.Panel>
                        </StyledCollapse>

                        {/* Notification Settings for Personal Subscription */}
                        {personalSubscriptionEnabled && <NotificationSettingsSection isPersonal groupUrn={undefined} />}
                    </div>
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
                            <SimpleSelect
                                isMultiSelect={false}
                                placeholder="Select groups to notify..."
                                values={selectedGroups}
                                onUpdate={onUpdateGroups}
                                options={groupOptions}
                                width="full"
                                showClear
                            />
                        </GroupSelectContainer>

                        {selectedGroups.length > 0 && (
                            <>
                                {!groupNotificationTypesExpanded && groupCheckedKeys.length > 0 && (
                                    <SummaryLabel>
                                        {groupCheckedKeys.length ? <SummaryLabelCheck size={16} /> : null}{' '}
                                        {generateSummaryText(groupCheckedKeys)}
                                    </SummaryLabel>
                                )}
                                <StyledCollapse
                                    ghost
                                    expandIcon={({ isActive }) => (
                                        <StyledIcon>{isActive ? <CaretDown /> : <CaretRight />}</StyledIcon>
                                    )}
                                    onChange={(activeKey) => {
                                        setGroupNotificationTypesExpanded(
                                            Array.isArray(activeKey) ? activeKey.length > 0 : !!activeKey,
                                        );
                                    }}
                                >
                                    <Collapse.Panel
                                        header={<CollapseHeader>Select Notification Types</CollapseHeader>}
                                        key="group-notifications"
                                    >
                                        <TreeContainer>
                                            <Tree
                                                checkable
                                                checkedKeys={groupCheckedKeys}
                                                onCheck={onGroupCheck}
                                                treeData={treeData}
                                                defaultExpandAll
                                            />
                                        </TreeContainer>
                                    </Collapse.Panel>
                                </StyledCollapse>

                                {/* Notification Settings for Group Subscription */}
                                <NotificationSettingsSection isPersonal={false} groupUrn={selectedGroups[0]} />
                            </>
                        )}
                    </>
                )}
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
