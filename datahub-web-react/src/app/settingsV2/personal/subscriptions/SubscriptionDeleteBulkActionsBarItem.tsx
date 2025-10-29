import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { useBulkDeleteSubscriptions } from '@app/settingsV2/personal/subscriptions/useBulkDeleteSubscriptions';
import { Button, Modal, Text } from '@src/alchemy-components';

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

interface Props {
    selectedUrns: string[];
    setSelectedUrns: React.Dispatch<React.SetStateAction<string[]>>;
    refetch: () => void;
    isPersonal: boolean;
}

export const SubscriptionDeleteBulkActionsBarItem = ({ selectedUrns, setSelectedUrns, refetch, isPersonal }: Props) => {
    const [showDeleteModal, setShowDeleteModal] = useState(false);
    const { bulkDeleteSubscriptionsByUrns, progress, resetProgress } = useBulkDeleteSubscriptions();

    const handleCancel = () => {
        setShowDeleteModal(false);
        resetProgress();
        // Keep failed subscriptions selected
        if (progress.failed.length > 0) {
            setSelectedUrns(progress.failed.map((f) => f.urn));
        }
    };

    const handleDelete = async () => {
        await bulkDeleteSubscriptionsByUrns(selectedUrns, isPersonal);
    };

    const handleRetry = async () => {
        // Retry only the failed subscriptions
        const failedUrns = progress.failed.map((f) => f.urn);
        resetProgress();
        await bulkDeleteSubscriptionsByUrns(failedUrns, isPersonal);
    };

    const handleCloseAfterSuccess = () => {
        setShowDeleteModal(false);
        resetProgress();
        setSelectedUrns([]);
        refetch();
    };

    const progressPercentage = progress.total > 0 ? Math.round((progress.completed / progress.total) * 100) : 0;
    const isInProgress = progress.hasStarted && progress.completed < progress.total;
    const isCompleted = progress.total > 0 && progress.completed === progress.total;
    const hasFailures = progress.failed.length > 0;
    const allSucceeded = isCompleted && !hasFailures;

    // Determine modal state
    const getModalTitle = () => {
        if (allSucceeded) return 'Deletion Complete';
        if (isCompleted && hasFailures) return 'Deletion Completed with Errors';
        return 'Delete Subscriptions';
    };

    const getModalSubtitle = () => {
        if (allSucceeded) return `All ${progress.total} subscriptions deleted successfully`;
        if (isCompleted && hasFailures) {
            return `${progress.successful.length} succeeded, ${progress.failed.length} failed`;
        }
        return `Are you sure you want to delete ${selectedUrns.length} subscription${
            selectedUrns.length > 1 ? 's' : ''
        }?`;
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
                text: isInProgress ? 'Deleting...' : 'Delete',
                color: 'red' as const,
                onClick: handleDelete,
                disabled: isInProgress,
                isLoading: isInProgress,
            },
        ];
    };

    return (
        <>
            <Button color="red" variant="filled" onClick={() => setShowDeleteModal(true)}>
                Delete
            </Button>
            {showDeleteModal && (
                <Modal
                    title={getModalTitle()}
                    subtitle={getModalSubtitle()}
                    {...(!isInProgress && { onCancel: handleCancel })}
                    buttons={getModalButtons()}
                >
                    <Text>This action cannot be undone.</Text>
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
                                        <ErrorItem key={`${failure.urn}`}>
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
