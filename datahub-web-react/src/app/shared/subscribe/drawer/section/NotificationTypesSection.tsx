import { Tree, Typography } from 'antd';
import { DataNode } from 'antd/es/tree';
import React, { Key, useCallback, useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import AssertionSubscriptionAlert from '@app/shared/subscribe/drawer/section/AssertionSubscriptionAlert';
import {
    checkIsKeyBeingToggledForAssertionSubscriptionsAtAssetLevel,
    getEntityChangeTypeWithName,
    useRemoveAssertionFromAssetLevelSubscription,
} from '@app/shared/subscribe/drawer/section/utils';
import useDrawerActions from '@app/shared/subscribe/drawer/state/actions';
import {
    selectCheckedKeys,
    selectExpandedKeys,
    selectKeysWithFilteringCleared,
    useDrawerSelector,
} from '@app/shared/subscribe/drawer/state/selectors';
import {
    ASSERTION_SUBSCRIPTION_RELATED_ENTITY_CHANGE_TYPES,
    getTreeDataForEntity,
} from '@app/shared/subscribe/drawer/utils';

import { Assertion, DataHubSubscription, EntityChangeType, EntityType } from '@types';

const NotificationTypesContainer = styled.div`
    margin-top: 32px;
    margin-left: 8px;
`;

const NotifyActorTitleContainer = styled.div`
    margin-bottom: 8px;
`;

const NotifyActorTitle = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
`;

const TreeContainer = styled.div`
    .ant-tree-checkbox .ant-tree-checkbox-inner {
        border-color: ${ANTD_GRAY[7]};
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

const ASSERTION_SUBSCRIPTION_RELATED_ENTITY_CHANGE_TYPES_AS_STRINGS =
    ASSERTION_SUBSCRIPTION_RELATED_ENTITY_CHANGE_TYPES.map((e) => e.valueOf().toString());

interface Props {
    entityUrn: string;
    entityType: EntityType;
    subscription?: DataHubSubscription;
    forSubResource?: { assertion?: Assertion };
    onClose();
}

const NotificationTypesSection = ({ entityUrn, entityType, forSubResource, subscription, onClose }: Props) => {
    const checkedKeys = useDrawerSelector(selectCheckedKeys);
    const expandedKeys = useDrawerSelector(selectExpandedKeys);

    // These are the keys user has cleared filters for
    // ie. if they've cleared all assertion filters for a specific key (EntityChangeType)
    const keysWithFilteringCleared = useDrawerSelector(selectKeysWithFilteringCleared);

    const actions = useDrawerActions();

    const [alertVisibleForEntityChangeType, setAlertVisibleForEntityChangeType] = useState<EntityChangeType>();
    const onRemoveAssertionFromAssetLevelSubscription = useRemoveAssertionFromAssetLevelSubscription({
        entityUrn,
        entityType,
    });

    const onExpand = useCallback(
        (expandedKeysValue: Key[]) => {
            actions.setExpandedNotificationTypes(expandedKeysValue);
        },
        [actions],
    );

    const showToggleAssetLevelSubscriptionAlert = (entityChangeType: EntityChangeType) => {
        setAlertVisibleForEntityChangeType(entityChangeType);
    };

    const tryAddKeysWithFiltersCleared = (nodeBeingToggled: DataNode) => {
        let keysToClear: Key[] | undefined =
            nodeBeingToggled.key === 'assertion_changes'
                ? nodeBeingToggled.children?.map((child) => child.key)
                : undefined;
        if (ASSERTION_SUBSCRIPTION_RELATED_ENTITY_CHANGE_TYPES_AS_STRINGS.includes(String(nodeBeingToggled.key))) {
            keysToClear = [nodeBeingToggled.key];
        }

        if (keysToClear?.length) {
            actions.setNotificationTypesWithFiltersCleared([...keysWithFilteringCleared, ...keysToClear]);
        }
    };

    // Handle user checking/unchecking a key
    const onCheck = (checkedKeysValue: any, _info: any) => {
        const { node } = _info as { node: DataNode };
        // 1. If this view is for managing an assertions's subscriptions...
        // ...and the user has attempted to uncheck an EntityChangeType that's set at the entity-level and not the assertion level,
        // then we need to alert the user they need to switch to the top entity-level subscription management view to perform this.
        if (
            forSubResource?.assertion &&
            checkIsKeyBeingToggledForAssertionSubscriptionsAtAssetLevel(node.key, subscription)
        ) {
            // NOTE: for now we don't let you manage across assertion change types if you've got asset level subscriptions
            if (node.key === 'assertion_changes') {
                return;
            }
            const maybeEntityChangeType = getEntityChangeTypeWithName(node.key as string);
            if (!maybeEntityChangeType) {
                // Should never happen
                alert('Could not find an entity change type matching the selected key.');
                return;
            }
            showToggleAssetLevelSubscriptionAlert(maybeEntityChangeType);
            return;
        }

        // 2. Update state based on this check action
        actions.setNotificationTypes(checkedKeysValue);

        // 3. If this is for top entity-level subscription management, and the user clicks a change type that has subresource filters
        // on it, we need to clear all those filters; as this is now being set at the entity-level.
        if (!forSubResource?.assertion) {
            tryAddKeysWithFiltersCleared(node);
        }
    };

    const treeData: DataNode[] = getTreeDataForEntity(entityType, {
        forSubResource,
        subscription,
        keysWithFilteringCleared,
    });

    // If this view is being rendered for assertion subscriptions, expand the respective key by default
    const firstKeyInTree = treeData.length ? treeData[0].key : null;
    const isForSubResource = !!forSubResource;
    useEffect(() => {
        if (isForSubResource && firstKeyInTree) {
            // Trigger on next tick of event loop
            setTimeout(() => onExpand([firstKeyInTree]), 50);
        }
    }, [isForSubResource, firstKeyInTree, onExpand, checkedKeys]);

    return (
        <NotificationTypesContainer>
            <NotifyActorTitleContainer>
                <NotifyActorTitle>Send notifications for</NotifyActorTitle>
            </NotifyActorTitleContainer>
            <TreeContainer>
                <Tree
                    checkable
                    onExpand={onExpand}
                    expandedKeys={expandedKeys}
                    onCheck={onCheck}
                    checkedKeys={checkedKeys}
                    treeData={treeData}
                />
            </TreeContainer>
            <AssertionSubscriptionAlert
                visible={!!alertVisibleForEntityChangeType}
                onConfirm={() => {
                    if (!forSubResource?.assertion || !alertVisibleForEntityChangeType) {
                        // Should never happen
                        alert(
                            `Unexpected error. Could not get assertion to perform action on.\nPlease try again later.`,
                        );
                        return;
                    }
                    if (!subscription) {
                        // Should never happen
                        alert(`Unexpected error. Could not get subscription to update.\nPlease try again later.`);
                        return;
                    }
                    try {
                        onRemoveAssertionFromAssetLevelSubscription(
                            forSubResource.assertion,
                            alertVisibleForEntityChangeType,
                            subscription,
                        );
                        setAlertVisibleForEntityChangeType(undefined);
                        onClose();
                    } catch (e: unknown) {
                        // should never happen
                        const errorMessage = typeof e === 'object' && e !== null && 'message' in e ? e.message : '';
                        alert(`Could not unsubscribe from this assertion.\n${errorMessage}\nPlease try again later.`);
                    }
                }}
                onCancel={() => setAlertVisibleForEntityChangeType(undefined)}
            />
        </NotificationTypesContainer>
    );
};

export default NotificationTypesSection;
