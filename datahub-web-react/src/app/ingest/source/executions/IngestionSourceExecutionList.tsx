import React, { useEffect, useState } from 'react';
import { message, Modal } from 'antd';
import styled from 'styled-components';
import {
    useGetIngestionSourceQuery,
    useCancelIngestionExecutionRequestMutation,
    useRollbackIngestionMutation,
} from '../../../../graphql/ingestion.generated';
import { Message } from '../../../shared/Message';
import { ExecutionDetailsModal } from './ExecutionRequestDetailsModal';
import IngestionExecutionTable from './IngestionExecutionTable';
import { ExecutionRequest } from '../../../../types.generated';
import { ROLLING_BACK, RUNNING } from '../utils';
import useRefreshIngestionData from './useRefreshIngestionData';
import { SearchCfg } from '../../../../conf';

const ListContainer = styled.div`
    margin-left: 28px;
`;

export function isExecutionRequestActive(executionRequest: ExecutionRequest) {
    return executionRequest.result?.status === RUNNING || executionRequest.result?.status === ROLLING_BACK;
}

type Props = {
    urn: string;
    isExpanded: boolean;
    lastRefresh: number;
    onRefresh: () => void;
};

export const IngestionSourceExecutionList = ({ urn, isExpanded, lastRefresh, onRefresh }: Props) => {
    const [focusExecutionUrn, setFocusExecutionUrn] = useState<undefined | string>(undefined);
    const [page, setPage] = useState(1);
    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);

    const start: number = (page - 1) * numResultsPerPage;

    const { loading, data, error, refetch } = useGetIngestionSourceQuery({
        variables: {
            urn,
            runStart: start,
            runCount: numResultsPerPage,
        },
    });

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    function hasActiveExecution() {
        return !!data?.ingestionSource?.executions?.executionRequests?.find((request) =>
            isExecutionRequestActive(request as ExecutionRequest),
        );
    }
    useRefreshIngestionData(refetch, hasActiveExecution);

    const [cancelExecutionRequestMutation] = useCancelIngestionExecutionRequestMutation();
    const [rollbackIngestion] = useRollbackIngestionMutation();

    useEffect(() => {
        if (isExpanded) {
            refetch();
        }
    }, [lastRefresh, isExpanded, refetch]);

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

    function handleRollbackExecution(runId: string) {
        Modal.confirm({
            title: `Confirm Rollback`,
            content: (
                <div>
                    Rolling back this ingestion run will remove any new data ingested during the run. This may exclude
                    data that was previously extracted, but did not change during this run.
                    <br />
                    <br /> Are you sure you want to continue?
                </div>
            ),
            onOk() {
                message.loading('Requesting rollback...');
                rollbackIngestion({ variables: { input: { runId } } })
                    .then(() => {
                        setTimeout(() => {
                            message.destroy();
                            refetch();
                            onRefresh();
                            message.success('Successfully requested ingestion rollback');
                        }, 2000);
                    })
                    .catch(() => {
                        message.error('Error requesting ingestion rollback');
                    });
            },
            onCancel() {},
            okText: 'Rollback',
            cancelText: 'Close',
            maskClosable: true,
            closable: true,
        });
    }

    const executionRequests = (data?.ingestionSource?.executions?.executionRequests as ExecutionRequest[]) || [];
    const totalExecution = data?.ingestionSource?.executions?.total || 0;
    const pageSize = data?.ingestionSource?.executions?.count || 0;
    const pageStart = data?.ingestionSource?.executions?.start || 0;
    const lastResultIndex = pageStart + pageSize > totalExecution ? totalExecution : pageStart + pageSize;

    return (
        <ListContainer>
            {!data && loading && <Message type="loading" content="Loading executions..." />}
            {error && (
                <Message type="error" content="Failed to load ingestion executions! An unexpected error occurred." />
            )}
            <IngestionExecutionTable
                onChangePage={onChangePage}
                executionRequests={executionRequests}
                totalExecution={totalExecution}
                page={page}
                pageSize={numResultsPerPage}
                lastResultIndex={lastResultIndex}
                setFocusExecutionUrn={setFocusExecutionUrn}
                handleCancelExecution={handleCancelExecution}
                handleViewDetails={handleViewDetails}
                handleRollbackExecution={handleRollbackExecution}
                setNumResultsPerPage={setNumResultsPerPage}
            />
            {focusExecutionUrn && (
                <ExecutionDetailsModal
                    urn={focusExecutionUrn}
                    open={focusExecutionUrn !== undefined}
                    onClose={() => setFocusExecutionUrn(undefined)}
                />
            )}
        </ListContainer>
    );
};
