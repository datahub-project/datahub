import { CodeOutlined, CopyOutlined, DeleteOutlined, PlusOutlined, RedoOutlined } from '@ant-design/icons';
import React, { useCallback, useEffect, useState } from 'react';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import { Button, Empty, Image, message, Modal, Pagination, Select, Tooltip, Typography } from 'antd';
import styled from 'styled-components';
import cronstrue from 'cronstrue';
import { blue } from '@ant-design/colors';
import {
    useCreateIngestionExecutionRequestMutation,
    useCreateIngestionSourceMutation,
    useDeleteIngestionSourceMutation,
    useListIngestionSourcesQuery,
    useUpdateIngestionSourceMutation,
} from '../../../graphql/ingestion.generated';
import { Message } from '../../shared/Message';
import TabToolbar from '../../entity/shared/components/styled/TabToolbar';
import { IngestionSourceBuilderModal } from './builder/IngestionSourceBuilderModal';
import { StyledTable } from '../../entity/shared/components/styled/StyledTable';
import { IngestionSourceExecutionList } from './IngestionSourceExecutionList';
import {
    CLI_EXECUTOR_ID,
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
    RUNNING,
    sourceTypeToIconUrl,
} from './utils';
import { DEFAULT_EXECUTOR_ID, SourceBuilderState } from './builder/types';
import { UpdateIngestionSourceInput } from '../../../types.generated';
import { capitalizeFirstLetter } from '../../shared/textUtil';
import { SearchBar } from '../../search/SearchBar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ExecutionDetailsModal } from './ExecutionRequestDetailsModal';
import { ANTD_GRAY } from '../../entity/shared/constants';
import RecipeViewerModal from './RecipeViewerModal';

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

const StatusContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const StatusButton = styled(Button)`
    padding: 0px;
    margin: 0px;
`;

const ActionButtonContainer = styled.div`
    display: flex;
    justify-content: right;
`;

const TypeWrapper = styled.div`
    align-items: center;
    display: flex;
`;

const CliBadge = styled.span`
    margin-left: 20px;
    border-radius: 15px;
    border: 1px solid ${ANTD_GRAY[8]};
    padding: 1px 4px;
    font-size: 10px;

    font-size: 8px;
    font-weight: bold;
    letter-spacing: 0.5px;
    border: 1px solid ${blue[6]};
    color: ${blue[6]};

    svg {
        display: none;
        margin-right: 5px;
    }
`;

const StyledSourceTable = styled(StyledTable)`
    .cliIngestion {
        td {
            background-color: ${ANTD_GRAY[2]} !important;
        }
    }
` as typeof StyledTable;

const StyledSelect = styled(Select)`
    margin-right: 15px;
    min-width: 75px;
`;

const FilterWrapper = styled.div`
    display: flex;
`;

enum IngestionSourceType {
    ALL,
    UI,
    CLI,
}

function shouldIncludeSource(source: any, sourceFilter: IngestionSourceType) {
    if (sourceFilter === IngestionSourceType.CLI) {
        return source.config.executorId === CLI_EXECUTOR_ID;
    }
    if (sourceFilter === IngestionSourceType.UI) {
        return source.config.executorId !== CLI_EXECUTOR_ID;
    }
    return true;
}

const DEFAULT_PAGE_SIZE = 25;

const removeExecutionsFromIngestionSource = (source) => {
    if (source) {
        return {
            name: source.name,
            type: source.type,
            schedule: source.schedule,
            config: source.config,
        };
    }
    return undefined;
};

export const IngestionSourceList = () => {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const [isBuildingSource, setIsBuildingSource] = useState<boolean>(false);
    const [isViewingRecipe, setIsViewingRecipe] = useState<boolean>(false);
    const [focusSourceUrn, setFocusSourceUrn] = useState<undefined | string>(undefined);
    const [focusExecutionUrn, setFocusExecutionUrn] = useState<undefined | string>(undefined);
    const [lastRefresh, setLastRefresh] = useState(0);
    // Set of removed urns used to account for eventual consistency
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);
    const [refreshInterval, setRefreshInterval] = useState<NodeJS.Timeout | null>(null);
    const [sourceFilter, setSourceFilter] = useState(IngestionSourceType.ALL);

    // Ingestion Source Queries
    const { loading, error, data, refetch } = useListIngestionSourcesQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query,
            },
        },
    });
    const [createIngestionSource] = useCreateIngestionSourceMutation();
    const [updateIngestionSource] = useUpdateIngestionSourceMutation();

    // Execution Request queries
    const [createExecutionRequestMutation] = useCreateIngestionExecutionRequestMutation();
    const [removeIngestionSourceMutation] = useDeleteIngestionSourceMutation();

    const totalSources = data?.listIngestionSources?.total || 0;
    const sources = data?.listIngestionSources?.ingestionSources || [];
    const filteredSources = sources.filter(
        (source) => !removedUrns.includes(source.urn) && shouldIncludeSource(source, sourceFilter),
    );
    const focusSource =
        (focusSourceUrn && filteredSources.find((source) => source.urn === focusSourceUrn)) || undefined;

    const onRefresh = useCallback(() => {
        refetch();
        // Used to force a re-render of the child execution request list.
        setLastRefresh(new Date().getMilliseconds());
    }, [refetch]);

    useEffect(() => {
        const runningSource = filteredSources.find((source) =>
            source.executions?.executionRequests.find((request) => request.result?.status === RUNNING),
        );
        if (runningSource) {
            if (!refreshInterval) {
                const interval = setInterval(onRefresh, 3000);
                setRefreshInterval(interval);
            }
        } else if (refreshInterval) {
            clearInterval(refreshInterval);
        }
    }, [filteredSources, refreshInterval, onRefresh]);

    const executeIngestionSource = (urn: string) => {
        createExecutionRequestMutation({
            variables: {
                input: {
                    ingestionSourceUrn: urn,
                },
            },
        })
            .then(() => {
                message.success({
                    content: `Successfully submitted ingestion execution request!`,
                    duration: 3,
                });
                setTimeout(() => onRefresh(), 3000);
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to submit ingestion execution request!: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    const onCreateOrUpdateIngestionSourceSuccess = () => {
        setTimeout(() => refetch(), 2000);
        setIsBuildingSource(false);
        setFocusSourceUrn(undefined);
    };

    const createOrUpdateIngestionSource = (
        input: UpdateIngestionSourceInput,
        resetState: () => void,
        shouldRun?: boolean,
    ) => {
        if (focusSourceUrn) {
            // Update:
            updateIngestionSource({ variables: { urn: focusSourceUrn as string, input } })
                .then(() => {
                    message.success({
                        content: `Successfully updated ingestion source!`,
                        duration: 3,
                    });
                    onCreateOrUpdateIngestionSourceSuccess();
                    resetState();
                    if (shouldRun) executeIngestionSource(focusSourceUrn);
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to update ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                });
        } else {
            // Create
            createIngestionSource({ variables: { input } })
                .then((result) => {
                    message.loading({ content: 'Loading...', duration: 2 });
                    setTimeout(() => {
                        refetch();
                        message.success({
                            content: `Successfully created ingestion source!`,
                            duration: 3,
                        });
                        if (shouldRun && result.data?.createIngestionSource) {
                            executeIngestionSource(result.data.createIngestionSource);
                        }
                    }, 2000);
                    setIsBuildingSource(false);
                    setFocusSourceUrn(undefined);
                    resetState();
                    // onCreateOrUpdateIngestionSourceSuccess();
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

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const deleteIngestionSource = async (urn: string) => {
        removeIngestionSourceMutation({
            variables: { urn },
        })
            .then(() => {
                message.success({ content: 'Removed ingestion source.', duration: 2 });
                const newRemovedUrns = [...removedUrns, urn];
                setRemovedUrns(newRemovedUrns);
                setTimeout(function () {
                    refetch?.();
                }, 3000);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to remove ingestion source: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const onSubmit = (recipeBuilderState: SourceBuilderState, resetState: () => void, shouldRun?: boolean) => {
        createOrUpdateIngestionSource(
            {
                type: recipeBuilderState.type as string,
                name: recipeBuilderState.name as string,
                config: {
                    recipe: recipeBuilderState.config?.recipe as string,
                    version:
                        (recipeBuilderState.config?.version?.length &&
                            (recipeBuilderState.config?.version as string)) ||
                        undefined,
                    executorId:
                        (recipeBuilderState.config?.executorId?.length &&
                            (recipeBuilderState.config?.executorId as string)) ||
                        DEFAULT_EXECUTOR_ID,
                },
                schedule: recipeBuilderState.schedule && {
                    interval: recipeBuilderState.schedule?.interval as string,
                    timezone: recipeBuilderState.schedule?.timezone as string,
                },
            },
            resetState,
            shouldRun,
        );
    };

    const onEdit = (urn: string) => {
        setIsBuildingSource(true);
        setFocusSourceUrn(urn);
    };

    const onView = (urn: string) => {
        setIsViewingRecipe(true);
        setFocusSourceUrn(urn);
    };

    const onExecute = (urn: string) => {
        Modal.confirm({
            title: `Confirm Source Execution`,
            content: "Click 'Execute' to run this ingestion source.",
            onOk() {
                executeIngestionSource(urn);
            },
            onCancel() {},
            okText: 'Execute',
            maskClosable: true,
            closable: true,
        });
    };

    const onDelete = (urn: string) => {
        Modal.confirm({
            title: `Confirm Ingestion Source Removal`,
            content: `Are you sure you want to remove this ingestion source? Removing will terminate any scheduled ingestion runs.`,
            onOk() {
                deleteIngestionSource(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const onCancel = () => {
        setIsBuildingSource(false);
        setIsViewingRecipe(false);
        setFocusSourceUrn(undefined);
    };

    const tableColumns = [
        {
            title: 'Type',
            dataIndex: 'type',
            key: 'type',
            render: (type: string, record: any) => {
                const iconUrl = sourceTypeToIconUrl(type);
                const typeDisplayName = capitalizeFirstLetter(type);
                return (
                    <TypeWrapper>
                        {iconUrl ? (
                            <Tooltip overlay={typeDisplayName}>
                                <PreviewImage preview={false} src={iconUrl} alt={type || ''} />
                            </Tooltip>
                        ) : (
                            <Typography.Text strong>{typeDisplayName}</Typography.Text>
                        )}
                        {record.cliIngestion && (
                            <Tooltip title="This source is ingested from the command-line interface (CLI)">
                                <CliBadge>
                                    <CodeOutlined />
                                    CLI
                                </CliBadge>
                            </Tooltip>
                        )}
                    </TypeWrapper>
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
            render: (schedule: any, record: any) => {
                const tooltip = schedule && `Runs ${cronstrue.toString(schedule).toLowerCase()} (${record.timezone})`;
                return (
                    <Tooltip title={tooltip || 'Not scheduled'}>
                        <Typography.Text code>{schedule || 'None'}</Typography.Text>
                    </Tooltip>
                );
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
                return <Typography.Text>{localTime || 'N/A'}</Typography.Text>;
            },
        },
        {
            title: 'Last Status',
            dataIndex: 'lastExecStatus',
            key: 'lastExecStatus',
            render: (status: any, record) => {
                const Icon = getExecutionRequestStatusIcon(status);
                const text = getExecutionRequestStatusDisplayText(status);
                const color = getExecutionRequestStatusDisplayColor(status);
                return (
                    <StatusContainer>
                        {Icon && <Icon style={{ color }} />}
                        <StatusButton type="link" onClick={() => setFocusExecutionUrn(record.lastExecUrn)}>
                            <Typography.Text strong style={{ color, marginLeft: 8 }}>
                                {text || 'N/A'}
                            </Typography.Text>
                        </StatusButton>
                    </StatusContainer>
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <ActionButtonContainer>
                    {navigator.clipboard && (
                        <Tooltip title="Copy Ingestion Source URN">
                            <Button
                                style={{ marginRight: 16 }}
                                icon={<CopyOutlined />}
                                onClick={() => {
                                    navigator.clipboard.writeText(record.urn);
                                }}
                            />
                        </Tooltip>
                    )}
                    {!record.cliIngestion && (
                        <Button style={{ marginRight: 16 }} onClick={() => onEdit(record.urn)}>
                            EDIT
                        </Button>
                    )}
                    {record.cliIngestion && (
                        <Button style={{ marginRight: 16 }} onClick={() => onView(record.urn)}>
                            VIEW
                        </Button>
                    )}
                    {record.lastExecStatus !== RUNNING && (
                        <Button
                            disabled={record.cliIngestion}
                            style={{ marginRight: 16 }}
                            onClick={() => onExecute(record.urn)}
                        >
                            EXECUTE
                        </Button>
                    )}
                    {record.lastExecStatus === RUNNING && (
                        <Button style={{ marginRight: 16 }} onClick={() => setFocusExecutionUrn(record.lastExecUrn)}>
                            DETAILS
                        </Button>
                    )}
                    <Button onClick={() => onDelete(record.urn)} type="text" shape="circle" danger>
                        <DeleteOutlined />
                    </Button>
                </ActionButtonContainer>
            ),
        },
    ];

    const tableData = filteredSources?.map((source) => ({
        urn: source.urn,
        type: source.type,
        name: source.name,
        schedule: source.schedule?.interval,
        timezone: source.schedule?.timezone,
        execCount: source.executions?.total || 0,
        lastExecUrn:
            source.executions?.total && source.executions?.total > 0 && source.executions?.executionRequests[0].urn,
        lastExecTime:
            source.executions?.total &&
            source.executions?.total > 0 &&
            source.executions?.executionRequests[0].result?.startTimeMs,
        lastExecStatus:
            source.executions?.total &&
            source.executions?.total > 0 &&
            source.executions?.executionRequests[0].result?.status,
        cliIngestion: source.config.executorId === CLI_EXECUTOR_ID,
    }));

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading ingestion sources..." />}
            {error &&
                message.error({ content: `Failed to load ingestion sources! \n ${error.message || ''}`, duration: 3 })}
            <SourceContainer>
                <TabToolbar>
                    <div>
                        <Button type="text" onClick={() => setIsBuildingSource(true)}>
                            <PlusOutlined /> Create new source
                        </Button>
                        <Button type="text" onClick={onRefresh}>
                            <RedoOutlined /> Refresh
                        </Button>
                    </div>
                    <FilterWrapper>
                        <StyledSelect
                            value={sourceFilter}
                            onChange={(selection) => setSourceFilter(selection as IngestionSourceType)}
                        >
                            <Select.Option value={IngestionSourceType.ALL}>All</Select.Option>
                            <Select.Option value={IngestionSourceType.UI}>UI</Select.Option>
                            <Select.Option value={IngestionSourceType.CLI}>CLI</Select.Option>
                        </StyledSelect>

                        <SearchBar
                            initialQuery={query || ''}
                            placeholderText="Search sources..."
                            suggestions={[]}
                            style={{
                                maxWidth: 220,
                                padding: 0,
                            }}
                            inputStyle={{
                                height: 32,
                                fontSize: 12,
                            }}
                            onSearch={() => null}
                            onQueryChange={(q) => setQuery(q)}
                            entityRegistry={entityRegistry}
                        />
                    </FilterWrapper>
                </TabToolbar>
                <StyledSourceTable
                    columns={tableColumns}
                    dataSource={tableData}
                    rowKey="urn"
                    rowClassName={(record, _) => (record.cliIngestion ? 'cliIngestion' : '')}
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
                initialState={removeExecutionsFromIngestionSource(focusSource)}
                visible={isBuildingSource}
                onSubmit={onSubmit}
                onCancel={onCancel}
            />
            {isViewingRecipe && <RecipeViewerModal recipe={focusSource?.config.recipe} onCancel={onCancel} />}
            {focusExecutionUrn && (
                <ExecutionDetailsModal
                    urn={focusExecutionUrn}
                    visible
                    onClose={() => setFocusExecutionUrn(undefined)}
                />
            )}
        </>
    );
};
