import React, { useState } from 'react';
import { Button, Empty, Image, message, Modal, Pagination, Tooltip, Typography } from 'antd';
import styled from 'styled-components';
import {
    CheckCircleOutlined,
    CloseCircleOutlined,
    DeleteOutlined,
    ExclamationCircleFilled,
    LoadingOutlined,
    PlusOutlined,
    RedoOutlined,
} from '@ant-design/icons';
import {
    useCreateIngestionExecutionRequestMutation,
    useCreateIngestionSourceMutation,
    useDeleteIngestionSourceMutation,
    useListIngestionSourcesQuery,
    useUpdateIngestionSourceMutation,
} from '../../graphql/ingestion.generated';
import { Message } from '../shared/Message';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import { IngestionSourceBuilderModal } from './IngestionSourceBuilderModal';
import { StyledTable } from '../entity/shared/components/styled/StyledTable';
import { REDESIGN_COLORS } from '../entity/shared/constants';
import { IngestionSourceExecutionList } from './IngestionSourceExecutionList';
import { sourceTypeToIconUrl } from './builder/utils';
import { BaseBuilderState } from './builder/types';
import { UpdateIngestionSourceInput } from '../../types.generated';

const SourceContainer = styled.div``;

const SourcePaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const PreviewImage = styled(Image)`
    max-height: 28px;
    width: auto;
    object-fit: contain;
    margin: 0px;
    background-color: transparent;
`;

const DEFAULT_PAGE_SIZE = 25;

export const IngestionSourceList = () => {
    const [page, setPage] = useState(1);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    // Whether or not there is an urn to show in the modal
    const [isEditingSource, setIsEditingSource] = useState<boolean>(false);
    const [focusSourceUrn, setFocusSourceUrn] = useState<undefined | string>(undefined);
    const [lastRefresh, setLastRefresh] = useState(0);

    // Set of removed urns used to account for eventual consistency
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);

    const { loading, error, data, refetch } = useListIngestionSourcesQuery({
        variables: {
            input: {
                start,
                count: pageSize,
            },
        },
        fetchPolicy: 'no-cache',
    });

    const [createIngestionSource] = useCreateIngestionSourceMutation();
    const [updateIngestionSource] = useUpdateIngestionSourceMutation();
    const [createExecutionRequestMutation] = useCreateIngestionExecutionRequestMutation();
    const [removeIngestionSourceMutation] = useDeleteIngestionSourceMutation();

    const totalSources = data?.listIngestionSources?.total || 0;
    const sources = data?.listIngestionSources?.ingestionSources || [];
    const filteredSources = sources.filter((user) => !removedUrns.includes(user.urn));
    const focusSource =
        (focusSourceUrn && filteredSources.find((source) => source.urn === focusSourceUrn)) || undefined;

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const onRefresh = () => {
        refetch();
        setLastRefresh(new Date().getMilliseconds());
    };

    const handleDelete = (urn: string) => {
        // Hack to deal with eventual consistency.
        const newRemovedUrns = [...removedUrns, urn];
        setRemovedUrns(newRemovedUrns);
        setTimeout(function () {
            refetch?.();
        }, 3000);
    };

    const onRemoveIngestionSource = async (urn: string) => {
        try {
            await removeIngestionSourceMutation({
                variables: { urn },
            });
            message.success({ content: 'Removed ingestion source.', duration: 2 });
            handleDelete(urn);
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to remove ingestion source: \n ${e.message || ''}`, duration: 3 });
            }
        }
    };

    const handleRemoveIngestionSource = (urn: string) => {
        Modal.confirm({
            title: `Confirm Ingestion Source Removal`,
            content: `Are you sure you want to remove this ingestion source? Removing will terminate all scheduled ingestion runs.`,
            onOk() {
                onRemoveIngestionSource(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const handleEdit = (urn: string) => {
        setIsEditingSource(true);
        setFocusSourceUrn(urn);
    };

    const onExecuteIngestionSource = (urn: string) => {
        createExecutionRequestMutation({
            variables: {
                input: {
                    ingestionSourceUrn: urn,
                },
            },
        })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to execute ingestion source!: \n ${e.message || ''}`,
                    duration: 3,
                });
            })
            .finally(() => {
                message.success({
                    content: `Successfully submitted ingestion job!`,
                    duration: 3,
                });
                setIsEditingSource(false);
                // Refresh once a job was submitted.
                setTimeout(() => onRefresh(), 3000);
            });
    };

    const handleExecute = (urn: string) => {
        Modal.confirm({
            title: `Confirm Source Execution`,
            content: "Click 'Execute' to run this ingestion source.",
            onOk() {
                onExecuteIngestionSource(urn);
            },
            onCancel() {},
            okText: 'Execute',
            maskClosable: true,
            closable: true,
        });
    };

    const createOrUpdateIngestionSource = (input: UpdateIngestionSourceInput) => {
        if (focusSourceUrn) {
            // Update:
            updateIngestionSource({ variables: { urn: focusSourceUrn as string, input } })
                .then(() => {
                    message.success({
                        content: `Successfully updated ingestion source!`,
                        duration: 3,
                    });
                    setIsEditingSource(false);
                    setFocusSourceUrn(undefined);
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to update ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                });
        } else {
            // Create:
            createIngestionSource({ variables: { input } })
                .then(() => {
                    message.success({
                        content: `Successfully created ingestion source!`,
                        duration: 3,
                    });
                    setIsEditingSource(false);
                    setFocusSourceUrn(undefined);
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to create ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                });
        }
    };

    const handleSubmit = (recipeBuilderState: BaseBuilderState) => {
        // 1. validateState()
        // 2. Create a new ingestion source.
        createOrUpdateIngestionSource({
            type: recipeBuilderState.type as string,
            name: recipeBuilderState.name || '', // Validate that this is not null.
            config: {
                recipe: recipeBuilderState.config?.recipe as string,
            },
            schedule: {
                interval: recipeBuilderState.schedule?.interval as string,
                timezone: recipeBuilderState.schedule?.timezone as string,
                startTimeMs: recipeBuilderState.schedule?.startTimeMs,
            },
        });
    };

    const handleCancel = () => {
        setIsEditingSource(false);
        setFocusSourceUrn(undefined);
    };

    const tableColumns = [
        {
            title: 'Type',
            dataIndex: 'type',
            key: 'type',
            render: (type: string) => {
                // TODO: Add icon.
                const iconUrl = sourceTypeToIconUrl(type);
                return (
                    <>
                        {(iconUrl && (
                            <Tooltip overlay={<>{type}</>}>
                                <PreviewImage preview={false} src={iconUrl} alt={type || ''} />
                            </Tooltip>
                        )) ||
                            type}
                    </>
                );
            },
        },
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (name: string) => name || '',
        },
        {
            title: 'Schedule',
            dataIndex: 'schedule',
            key: 'schedule',
            render: (schedule: any) => {
                return <Typography.Text code>{schedule || 'N/A'}</Typography.Text>;
            },
        },
        {
            title: 'Execution Count',
            dataIndex: 'execCount',
            key: 'execCount',
            render: (execCount: any) => {
                return <Typography.Text>{execCount || '0'}</Typography.Text>;
            },
        },
        {
            title: 'Last Execution',
            dataIndex: 'lastExecTime',
            key: 'lastExecTime',
            render: (time: any) => {
                const executionDate = time && new Date(time);
                const localTime =
                    executionDate && `${executionDate.toLocaleDateString()} at ${executionDate.toLocaleTimeString()}`;
                return <Typography.Text>{localTime || 'Never'}</Typography.Text>;
            },
        },
        {
            title: 'Last Status',
            dataIndex: 'lastExecStatus',
            key: 'lastExecStatus',
            render: (status: any) => {
                const Icon =
                    (status === 'RUNNING' && LoadingOutlined) ||
                    (status === 'SUCCESS' && CheckCircleOutlined) ||
                    (status === 'FAILURE' && CloseCircleOutlined) ||
                    (status === 'TIMEOUT' && ExclamationCircleFilled);
                const text =
                    (status === 'RUNNING' && 'Running') ||
                    (status === 'SUCCESS' && 'Succeeded') ||
                    (status === 'FAILURE' && 'Failed') ||
                    (status === 'TIMEOUT' && 'Timed Out');
                const color =
                    (status === 'RUNNING' && REDESIGN_COLORS.BLUE) ||
                    (status === 'SUCCESS' && 'green') ||
                    (status === 'FAILURE' && 'red') ||
                    (status === 'TIMEOUT' && 'yellow') ||
                    REDESIGN_COLORS.GREY;
                return (
                    <>
                        <div style={{ display: 'flex', justifyContent: 'left', alignItems: 'center' }}>
                            {Icon && <Icon style={{ color }} />}
                            <Typography.Text strong style={{ color, marginLeft: 8 }}>
                                {text || 'N/A'}
                            </Typography.Text>
                        </div>
                    </>
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <div style={{ display: 'flex', justifyContent: 'right' }}>
                    <div style={{ display: 'flex' }}>
                        <Button style={{ marginRight: 16 }} onClick={() => handleEdit(record.urn)}>
                            EDIT
                        </Button>
                        <Button
                            disabled={record.lastExecStatus === 'RUNNING'}
                            style={{ marginRight: 16 }}
                            onClick={() => handleExecute(record.urn)}
                        >
                            EXECUTE
                        </Button>
                    </div>
                    <Button onClick={() => handleRemoveIngestionSource(record.urn)} type="text" shape="circle" danger>
                        <DeleteOutlined />
                    </Button>
                </div>
            ),
        },
    ];

    const tableData = filteredSources?.map((source) => ({
        urn: source.urn,
        type: 'mysql', // TODO with logo.
        name: source.name, // TODO: DisplayName -> Name
        schedule: source.schedule?.interval || 'undefined',
        execCount: source.executions?.total || 0,
        lastExecTime:
            source.executions?.total &&
            source.executions?.total > 0 &&
            source.executions?.executionRequests[0].result?.startTimeMs,
        lastExecStatus:
            source.executions?.total &&
            source.executions?.total > 0 &&
            source.executions?.executionRequests[0].result?.status,
    }));

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading ingestion sources..." />}
            {error && message.error('Failed to load ingestion sources :(')}
            <SourceContainer>
                <TabToolbar>
                    <div>
                        <Button type="text" onClick={() => setIsEditingSource(true)}>
                            <PlusOutlined /> Create new source
                        </Button>
                        <Button type="text" onClick={onRefresh}>
                            <RedoOutlined /> Refresh
                        </Button>
                    </div>
                </TabToolbar>
                <StyledTable
                    columns={tableColumns}
                    dataSource={tableData}
                    rowKey="urn"
                    locale={{
                        emptyText: <Empty description="No Ingestion Sources!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    expandable={{
                        expandedRowRender: (record) => {
                            return (
                                <IngestionSourceExecutionList
                                    urn={record.urn}
                                    lastRefresh={lastRefresh}
                                    onRefresh={onRefresh}
                                />
                            );
                        },
                        rowExpandable: (record) => {
                            return record.execCount > 0;
                        },
                        defaultExpandAllRows: false,
                        indentSize: 0,
                    }}
                    pagination={false}
                />
                <SourcePaginationContainer>
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalSources}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </SourcePaginationContainer>
            </SourceContainer>
            <IngestionSourceBuilderModal
                initialState={focusSource}
                visible={isEditingSource}
                onSubmit={handleSubmit}
                onCancel={handleCancel}
            />
        </>
    );
};
