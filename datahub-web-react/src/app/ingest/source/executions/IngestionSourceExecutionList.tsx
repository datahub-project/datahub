import React, { useEffect, useState } from 'react';
import { message, Modal } from 'antd';
import styled from 'styled-components';
import {
    useGetIngestionSourceQuery,
    useCancelIngestionExecutionRequestMutation,
} from '../../../../graphql/ingestion.generated';
import { Message } from '../../../shared/Message';
import { ExecutionDetailsModal } from '../ExecutionRequestDetailsModal';
import IngestionExecutionTable from './IngestionExecutionTable';
import { ExecutionRequest } from '../../../../types.generated';

const ListContainer = styled.div`
    margin-left: 28px;
`;

type Props = {
    urn: string;
    lastRefresh: number;
    onRefresh: () => void;
};

export const IngestionSourceExecutionList = ({ urn, lastRefresh, onRefresh }: Props) => {
    const [focusExecutionUrn, setFocusExecutionUrn] = useState<undefined | string>(undefined);

    const start = 0;
    const count = 10; // Load 10 items at a time.

    const { loading, data, error, refetch } = useGetIngestionSourceQuery({
        variables: {
            urn,
            runStart: start,
            runCount: count,
        },
    });

    const [cancelExecutionRequestMutation] = useCancelIngestionExecutionRequestMutation();

    useEffect(() => {
        refetch();
    }, [lastRefresh, refetch]);

    const handleViewDetails = (focusUrn: string) => {
        setFocusExecutionUrn(focusUrn);
    };

    const onCancelExecutionRequest = (executionUrn: string) => {
        cancelExecutionRequestMutation({
            variables: {
                input: {
                    ingestionSourceUrn: urn,
                    executionRequestUrn: executionUrn,
                },
            },
        })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to cancel execution!: \n ${e.message || ''}`,
                    duration: 3,
                });
            })
            .finally(() => {
                message.success({
                    content: `Successfully submitted cancellation request!`,
                    duration: 3,
                });
                // Refresh once a job was cancelled.
                setTimeout(() => onRefresh(), 2000);
            });
    };

    const handleCancelExecution = (executionUrn: string) => {
        Modal.confirm({
            title: `Confirm Cancel`,
            content:
                'Cancelling an running execution will NOT remove any data that has already been ingested. You can use the DataHub CLI to rollback this ingestion run.',
            onOk() {
                onCancelExecutionRequest(executionUrn);
            },
            onCancel() {},
            okText: 'Cancel',
            cancelText: 'Close',
            maskClosable: true,
            closable: true,
        });
    };

    const executionRequests = (data?.ingestionSource?.executions?.executionRequests as ExecutionRequest[]) || [];

    return (
        <ListContainer>
            {!data && loading && <Message type="loading" content="Loading executions..." />}
            {error && message.error('Failed to load executions :(')}
            <IngestionExecutionTable
                executionRequests={executionRequests}
                setFocusExecutionUrn={setFocusExecutionUrn}
                handleCancelExecution={handleCancelExecution}
                handleViewDetails={handleViewDetails}
            />
            {focusExecutionUrn && (
                <ExecutionDetailsModal
                    urn={focusExecutionUrn}
                    visible={focusExecutionUrn !== undefined}
                    onClose={() => setFocusExecutionUrn(undefined)}
                />
            )}
        </ListContainer>
    );
};
