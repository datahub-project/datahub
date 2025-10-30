import { Tree } from 'antd';
import { DataNode } from 'antd/es/tree';
import React, { Key, useCallback, useState } from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { useBulkUpdateSubscriptions } from '@app/settingsV2/personal/subscriptions/useBulkUpdateSubscriptions';
import { getEntityChangeTypesFromCheckedKeys, getTreeDataForEntity } from '@app/shared/subscribe/drawer/utils';
import { Button, Modal, Text } from '@src/alchemy-components';

import { EntityChangeDetailsInput, EntityType } from '@types';

const ProgressContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-top: 16px;
`;

const ProgressRow = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const ProgressBar = styled.div<{ $percentage: number }>`
    flex: 1;
    height: 8px;
    background-color: #f0f0f0;
    border-radius: 4px;
    overflow: hidden;

    &::after {
        content: '';
        display: block;
        height: 100%;
        width: ${(props) => props.$percentage}%;
        background-color: #4caf50;
        transition: width 0.3s ease;
    }
`;

const ErrorList = styled.div`
    margin-top: 8px;
    max-height: 100px;
    overflow-y: auto;
`;

const ErrorItem = styled.div`
    padding: 4px;
    font-size: 12px;
    color: #d32f2f;
`;

const TreeContainer = styled.div`
    margin-top: 16px;
    max-height: 300px;
    overflow-y: auto;
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

interface Props {
    selectedUrns: string[];
    setSelectedUrns: React.Dispatch<React.SetStateAction<string[]>>;
    refetch: () => void;
    isPersonal: boolean;
}

export const SubscriptionEditEventsBulkActionsBarItem = ({
    selectedUrns,
    setSelectedUrns,
    refetch,
    isPersonal,
}: Props) => {
    const [showEditModal, setShowEditModal] = useState(false);
    const [checkedKeys, setCheckedKeys] = useState<Key[]>([]);
    const [expandedKeys, setExpandedKeys] = useState<Key[]>([]);
    const { bulkUpdateSubscriptionsByUrns, progress, resetProgress } = useBulkUpdateSubscriptions();

    // Use EntityType.Dataset as it has the most comprehensive list of change types
    const treeData: DataNode[] = getTreeDataForEntity(EntityType.Dataset);

    const handleCancel = () => {
        setShowEditModal(false);
        resetProgress();
        setCheckedKeys([]);
        // Keep failed subscriptions selected
        if (progress.failed.length > 0) {
            setSelectedUrns(progress.failed.map((f) => f.urn));
        }
    };

    const handleUpdate = async () => {
        // Extract EntityChangeTypes from checked keys (filters out parent node keys)
        const entityChangeTypesArray = getEntityChangeTypesFromCheckedKeys(checkedKeys);
        // Convert to EntityChangeDetailsInput format
        const entityChangeTypes: EntityChangeDetailsInput[] = entityChangeTypesArray.map((changeType) => ({
            entityChangeType: changeType,
        }));

        await bulkUpdateSubscriptionsByUrns(selectedUrns, entityChangeTypes, isPersonal);
    };

    const handleRetry = async () => {
        // Retry only the failed subscriptions
        const failedUrns = progress.failed.map((f) => f.urn);
        const entityChangeTypesArray = getEntityChangeTypesFromCheckedKeys(checkedKeys);
        const entityChangeTypes: EntityChangeDetailsInput[] = entityChangeTypesArray.map((changeType) => ({
            entityChangeType: changeType,
        }));
        resetProgress();
        await bulkUpdateSubscriptionsByUrns(failedUrns, entityChangeTypes, isPersonal);
    };

    const handleCloseAfterSuccess = () => {
        setShowEditModal(false);
        resetProgress();
        setSelectedUrns([]);
        setCheckedKeys([]);
        refetch();
    };

    const onCheck = useCallback((checkedKeysValue: any) => {
        setCheckedKeys(checkedKeysValue);
    }, []);

    const onExpand = useCallback((expandedKeysValue: Key[]) => {
        setExpandedKeys(expandedKeysValue);
    }, []);

    const progressPercentage = progress.total > 0 ? Math.round((progress.completed / progress.total) * 100) : 0;
    const isInProgress = progress.hasStarted && progress.completed < progress.total;
    const isCompleted = progress.total > 0 && progress.completed === progress.total;
    const hasFailures = progress.failed.length > 0;
    const allSucceeded = isCompleted && !hasFailures;

    // Determine modal state
    const getModalTitle = () => {
        if (allSucceeded) return 'Update Complete';
        if (isCompleted && hasFailures) return 'Update Completed with Errors';
        return 'Edit Subscription Events';
    };

    const getModalSubtitle = () => {
        if (allSucceeded) return `All ${progress.total} subscriptions updated successfully`;
        if (isCompleted && hasFailures) {
            return `${progress.successful.length} succeeded, ${progress.failed.length} failed`;
        }
        return `Select the events you want to override for ${selectedUrns.length} subscription${
            selectedUrns.length > 1 ? 's' : ''
        }`;
    };

    const getModalButtons = () => {
        if (allSucceeded) {
            return [
                {
                    text: 'Close',
                    variant: 'filled' as const,
                    onClick: handleCloseAfterSuccess,
                },
            ];
        }
        if (isCompleted && hasFailures) {
            return [
                {
                    text: 'Retry Failed',
                    color: 'red' as const,
                    onClick: handleRetry,
                },
                {
                    text: 'Close',
                    variant: 'text' as const,
                    onClick: handleCancel,
                },
            ];
        }
        return [
            {
                text: 'Cancel',
                variant: 'text' as const,
                onClick: handleCancel,
                disabled: isInProgress,
            },
            {
                text: isInProgress ? 'Updating...' : 'Update',
                variant: 'filled' as const,
                onClick: handleUpdate,
                disabled: isInProgress || checkedKeys.length === 0,
                isLoading: isInProgress,
            },
        ];
    };

    return (
        <>
            <Button variant="filled" onClick={() => setShowEditModal(true)}>
                Edit Events
            </Button>
            {showEditModal && (
                <Modal
                    title={getModalTitle()}
                    subtitle={getModalSubtitle()}
                    {...(!isInProgress && { onCancel: handleCancel })}
                    buttons={getModalButtons()}
                >
                    {!progress.hasStarted && (
                        <>
                            <Text>
                                Selecting events will replace the existing event types for all selected subscriptions.
                            </Text>
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
                        </>
                    )}
                    {progress.hasStarted && (
                        <ProgressContainer>
                            <ProgressRow>
                                <Text size="sm">
                                    Progress: {progress.completed} / {progress.total}
                                </Text>
                                <ProgressBar $percentage={progressPercentage} />
                                <Text size="sm">{progressPercentage}%</Text>
                            </ProgressRow>
                            {progress.failed.length > 0 && (
                                <ErrorList>
                                    <Text size="sm" weight="bold">
                                        Failed ({progress.failed.length}):
                                    </Text>
                                    {progress.failed.map((failure) => (
                                        <ErrorItem key={failure.urn}>
                                            {failure.urn}: {failure.error}
                                        </ErrorItem>
                                    ))}
                                </ErrorList>
                            )}
                        </ProgressContainer>
                    )}
                </Modal>
            )}
        </>
    );
};
