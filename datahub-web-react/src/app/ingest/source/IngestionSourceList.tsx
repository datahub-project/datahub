import { PlusOutlined, RedoOutlined } from '@ant-design/icons';
import React, { useCallback, useEffect, useState } from 'react';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import { Button, message, Modal, Pagination, Select } from 'antd';
import styled from 'styled-components';
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
import { CLI_EXECUTOR_ID } from './utils';
import { DEFAULT_EXECUTOR_ID, SourceBuilderState } from './builder/types';
import { IngestionSource, UpdateIngestionSourceInput } from '../../../types.generated';
import { SearchBar } from '../../search/SearchBar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ExecutionDetailsModal } from './executions/ExecutionRequestDetailsModal';
import RecipeViewerModal from './RecipeViewerModal';
import IngestionSourceTable from './IngestionSourceTable';
import { scrollToTop } from '../../shared/searchUtils';
import useRefreshIngestionData from './executions/useRefreshIngestionData';
import { isExecutionRequestActive } from './executions/IngestionSourceExecutionList';

const SourceContainer = styled.div``;

const SourcePaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const StyledSelect = styled(Select)`
    margin-right: 15px;
    min-width: 75px;
`;

const FilterWrapper = styled.div`
    display: flex;
`;

export enum IngestionSourceType {
    ALL,
    UI,
    CLI,
}

export function shouldIncludeSource(source: any, sourceFilter: IngestionSourceType) {
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
    ) as IngestionSource[];
    const focusSource =
        (focusSourceUrn && filteredSources.find((source) => source.urn === focusSourceUrn)) || undefined;

    const onRefresh = useCallback(() => {
        refetch();
        // Used to force a re-render of the child execution request list.
        setLastRefresh(new Date().getTime());
    }, [refetch]);

    function hasActiveExecution() {
        return !!filteredSources.find((source) =>
            source.executions?.executionRequests.find((request) => isExecutionRequestActive(request)),
        );
    }
    useRefreshIngestionData(onRefresh, hasActiveExecution);

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
        scrollToTop();
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
                    debugMode: recipeBuilderState.config?.debugMode || false,
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

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading ingestion sources..." />}
            {error && (
                <Message type="error" content="Failed to load ingestion sources! An unexpected error occurred." />
            )}
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
                <IngestionSourceTable
                    lastRefresh={lastRefresh}
                    sources={filteredSources || []}
                    setFocusExecutionUrn={setFocusExecutionUrn}
                    onExecute={onExecute}
                    onEdit={onEdit}
                    onView={onView}
                    onDelete={onDelete}
                    onRefresh={onRefresh}
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
