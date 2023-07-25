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

    const start = 0;
    const count = 10; // Load 10 items at a time.

    const { loading, data, error, refetch } = useGetIngestionSourceQuery({
        variables: {
            urn,
            runStart: start,
            runCount: count,
        },
    });

    function hasActiveExecution() {
        return !!data?.ingestionSource?.executions?.executionRequests.find((request) =>
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
                    content: `取消请求失败!: \n ${e.message || ''}`,
                    duration: 3,
                });
            })
            .finally(() => {
                message.success({
                    content: `成功提交取消请求!`,
                    duration: 3,
                });
                // Refresh once a job was cancelled.
                setTimeout(() => onRefresh(), 2000);
            });
    };

    const handleCancelExecution = (executionUrn: string) => {
        Modal.confirm({
            title: `确认取消`,
            content:
                '取消正在执行的作业不会删除已经提交的数据。 您可以使用 DataHub CLI命令行工具来执行回滚操作.',
            onOk() {
                onCancelExecutionRequest(executionUrn);
            },
            onCancel() {},
            okText: '取消',
            cancelText: '关闭',
            maskClosable: true,
            closable: true,
        });
    };

    function handleRollbackExecution(runId: string) {
        Modal.confirm({
            title: `确认回滚`,
            content: (
                <div>
                    回滚本次集成作业生成的数据,之前的数据不会发生改变.
                    <br />
                    <br /> 确认回滚吗?
                </div>
            ),
            onOk() {
                message.loading('回滚请求中...');
                rollbackIngestion({ variables: { input: { runId } } })
                    .then(() => {
                        setTimeout(() => {
                            message.destroy();
                            refetch();
                            onRefresh();
                            message.success('成功提交回滚请求');
                        }, 2000);
                    })
                    .catch(() => {
                        message.error('回滚请求提交失败');
                    });
            },
            onCancel() {},
            okText: '回滚',
            cancelText: '关闭',
            maskClosable: true,
            closable: true,
        });
    }

    const executionRequests = (data?.ingestionSource?.executions?.executionRequests as ExecutionRequest[]) || [];

    return (
        <ListContainer>
            {!data && loading && <Message type="loading" content="加载运行作业..." />}
            {error && (
                <Message type="error" content="运行作业加载失败! 发生未知错误." />
            )}
            <IngestionExecutionTable
                executionRequests={executionRequests}
                setFocusExecutionUrn={setFocusExecutionUrn}
                handleCancelExecution={handleCancelExecution}
                handleViewDetails={handleViewDetails}
                handleRollbackExecution={handleRollbackExecution}
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
