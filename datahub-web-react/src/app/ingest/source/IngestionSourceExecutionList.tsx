import React, { useEffect, useState } from 'react';
import { CopyOutlined } from '@ant-design/icons';
import { Button, Empty, message, Modal, Typography, Tooltip } from 'antd';
import styled from 'styled-components';
import {
    useGetIngestionSourceQuery,
    useCancelIngestionExecutionRequestMutation,
} from '../../../graphql/ingestion.generated';
import { Message } from '../../shared/Message';
import { StyledTable } from '../../entity/shared/components/styled/StyledTable';
import { ExecutionDetailsModal } from './ExecutionRequestDetailsModal';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusIcon,
    getExecutionRequestStatusDisplayText,
    CLI_INGESTION_SOURCE,
    SCHEDULED_INGESTION_SOURCE,
    MANUAL_INGESTION_SOURCE,
} from './utils';

const ListContainer = styled.div`
    margin-left: 28px;
`;

const StatusContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const StatusButton = styled(Button)`
    padding: 0px;
    margin: 0px;
`;

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

    const [cancelExecutionRequestMutation] = useCancelIngestionExecutionRequestMutation();

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

    const tableColumns = [
        {
            title: 'Requested At',
            dataIndex: 'requestedAt',
            key: 'requestedAt',
            render: (time: string) => {
                const requestedDate = time && new Date(time);
                const localTime =
                    requestedDate && `${requestedDate.toLocaleDateString()} at ${requestedDate.toLocaleTimeString()}`;
                return <Typography.Text>{localTime || 'N/A'}</Typography.Text>;
            },
        },
        {
            title: 'Started At',
            dataIndex: 'executedAt',
            key: 'executedAt',
            render: (time: string) => {
                const executionDate = time && new Date(time);
                const localTime =
                    executionDate && `${executionDate.toLocaleDateString()} at ${executionDate.toLocaleTimeString()}`;
                return <Typography.Text>{localTime || 'N/A'}</Typography.Text>;
            },
        },
        {
            title: 'Duration (s)',
            dataIndex: 'duration',
            key: 'duration',
            render: (durationMs: number) => {
                const seconds = (durationMs && `${durationMs / 1000}s`) || 'N/A';
                return seconds;
            },
        },
        {
            title: 'Status',
            dataIndex: 'status',
            key: 'status',
            render: (status: any, record) => {
                const Icon = getExecutionRequestStatusIcon(status);
                const text = getExecutionRequestStatusDisplayText(status);
                const color = getExecutionRequestStatusDisplayColor(status);
                return (
                    <StatusContainer>
                        {Icon && <Icon style={{ color }} />}
                        <StatusButton type="link" onClick={() => setFocusExecutionUrn(record.urn)}>
                            <Typography.Text strong style={{ color, marginLeft: 8 }}>
                                {text || 'N/A'}
                            </Typography.Text>
                        </StatusButton>
                    </StatusContainer>
                );
            },
        },
        {
            title: 'Source',
            dataIndex: 'source',
            key: 'source',
            render: (source: string) => {
                return (
                    (source === MANUAL_INGESTION_SOURCE && 'Manual Execution') ||
                    (source === SCHEDULED_INGESTION_SOURCE && 'Scheduled Execution') ||
                    (source === CLI_INGESTION_SOURCE && 'CLI Execution') ||
                    'N/A'
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <div style={{ display: 'flex', justifyContent: 'right' }}>
                    {record.urn && navigator.clipboard && (
                        <Tooltip title="Copy Execution Request URN">
                            <Button
                                style={{ marginRight: 16 }}
                                icon={<CopyOutlined />}
                                onClick={() => {
                                    navigator.clipboard.writeText(record.urn);
                                }}
                            />
                        </Tooltip>
                    )}
                    {record.duration && (
                        <Button style={{ marginRight: 16 }} onClick={() => handleViewDetails(record.urn)}>
                            DETAILS
                        </Button>
                    )}
                    {record.status === 'RUNNING' && (
                        <Button style={{ marginRight: 16 }} onClick={() => handleCancelExecution(record.urn)}>
                            CANCEL
                        </Button>
                    )}
                </div>
            ),
        },
    ];

    const executions = data?.ingestionSource?.executions?.executionRequests;
    const tableData = executions?.map((execution) => ({
        urn: execution.urn,
        source: execution.input.source.type,
        requestedAt: execution.input?.requestedAt,
        executedAt: execution.result?.startTimeMs,
        duration: execution.result?.durationMs,
        status: execution.result?.status,
    }));

    return (
        <ListContainer>
            {!data && loading && <Message type="loading" content="Loading executions..." />}
            {error && message.error('Failed to load executions :(')}
            <StyledTable
                columns={tableColumns}
                dataSource={tableData}
                rowKey="id"
                locale={{
                    emptyText: <Empty description="No Executions found!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                pagination={false}
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
