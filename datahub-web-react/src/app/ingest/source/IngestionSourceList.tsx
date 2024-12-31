import { PlusOutlined, RedoOutlined } from '@ant-design/icons';
import React, { useCallback, useEffect, useState } from 'react';
import { debounce } from 'lodash';
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
import { addToListIngestionSourcesCache, CLI_EXECUTOR_ID, removeFromListIngestionSourcesCache } from './utils';
import { DEFAULT_EXECUTOR_ID, SourceBuilderState, StringMapEntryInput } from './builder/types';
import { IngestionSource, SortCriterion, SortOrder, UpdateIngestionSourceInput } from '../../../types.generated';
import { SearchBar } from '../../search/SearchBar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ExecutionDetailsModal } from './executions/ExecutionRequestDetailsModal';
import RecipeViewerModal from './RecipeViewerModal';
import IngestionSourceTable from './IngestionSourceTable';
import { scrollToTop } from '../../shared/searchUtils';
import useRefreshIngestionData from './executions/useRefreshIngestionData';
import { isExecutionRequestActive } from './executions/IngestionSourceExecutionList';
import analytics, { EventType } from '../../analytics';
import {
    INGESTION_CREATE_SOURCE_ID,
    INGESTION_REFRESH_SOURCES_ID,
} from '../../onboarding/config/IngestionOnboardingConfig';
import { ONE_SECOND_IN_MS } from '../../entity/shared/tabs/Dataset/Queries/utils/constants';
import { useCommandS } from './hooks';

const PLACEHOLDER_URN = 'placeholder-urn';

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

const SYSTEM_INTERNAL_SOURCE_TYPE = 'SYSTEM';

export enum IngestionSourceType {
    ALL,
    UI,
    CLI,
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
    const [sort, setSort] = useState<SortCriterion>();
    const [hideSystemSources, setHideSystemSources] = useState(true);

    /**
     * Show or hide system ingestion sources using a hidden command S command.
     */
    useCommandS(() => setHideSystemSources(!hideSystemSources));

    // Ingestion Source Default Filters
    const filters = hideSystemSources
        ? [{ field: 'sourceType', values: [SYSTEM_INTERNAL_SOURCE_TYPE], negated: true }]
        : [];
    if (sourceFilter !== IngestionSourceType.ALL) {
        filters.push({
            field: 'sourceExecutorId',
            values: [CLI_EXECUTOR_ID],
            negated: sourceFilter !== IngestionSourceType.CLI,
        });
    }

    // Ingestion Source Queries
    const { loading, error, data, client, refetch } = useListIngestionSourcesQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query: query?.length ? query : undefined,
                filters: filters.length ? filters : undefined,
                sort,
            },
        },
        fetchPolicy: (query?.length || 0) > 0 ? 'no-cache' : 'cache-first',
    });
    const [createIngestionSource] = useCreateIngestionSourceMutation();
    const [updateIngestionSource] = useUpdateIngestionSourceMutation();

    // Execution Request queries
    const [createExecutionRequestMutation] = useCreateIngestionExecutionRequestMutation();
    const [removeIngestionSourceMutation] = useDeleteIngestionSourceMutation();

    const totalSources = data?.listIngestionSources?.total || 0;
    const sources = data?.listIngestionSources?.ingestionSources || [];
    const filteredSources = sources.filter((source) => !removedUrns.includes(source.urn)) as IngestionSource[];
    const focusSource =
        (focusSourceUrn && filteredSources.find((source) => source.urn === focusSourceUrn)) || undefined;

    const onRefresh = useCallback(() => {
        refetch();
        // Used to force a re-render of the child execution request list.
        setLastRefresh(new Date().getTime());
    }, [refetch]);

    const debouncedSetQuery = debounce((newQuery: string | undefined) => {
        setQuery(newQuery);
    }, ONE_SECOND_IN_MS);

    function hasActiveExecution() {
        return !!filteredSources.find((source) =>
            source.executions?.executionRequests?.find((request) => isExecutionRequestActive(request)),
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
                analytics.event({
                    type: EventType.ExecuteIngestionSourceEvent,
                });
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

    const formatExtraArgs = (extraArgs): StringMapEntryInput[] => {
        if (extraArgs === null || extraArgs === undefined) return [];
        return extraArgs
            .filter((entry) => entry.value !== null && entry.value !== undefined && entry.value !== '')
            .map((entry) => ({ key: entry.key, value: entry.value }));
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
                    analytics.event({
                        type: EventType.UpdateIngestionSourceEvent,
                        sourceType: input.type,
                        interval: input.schedule?.interval,
                    });
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
                    const newSource = {
                        urn: result?.data?.createIngestionSource || PLACEHOLDER_URN,
                        name: input.name,
                        type: input.type,
                        config: null,
                        schedule: {
                            interval: input.schedule?.interval || null,
                            timezone: input.schedule?.timezone || null,
                        },
                        platform: null,
                        executions: null,
                    };
                    addToListIngestionSourcesCache(client, newSource, pageSize, query);
                    setTimeout(() => {
                        refetch();
                        analytics.event({
                            type: EventType.CreateIngestionSourceEvent,
                            sourceType: input.type,
                            interval: input.schedule?.interval,
                        });
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

    const deleteIngestionSource = (urn: string) => {
        removeFromListIngestionSourcesCache(client, urn, page, pageSize, query);
        removeIngestionSourceMutation({
            variables: { urn },
        })
            .then(() => {
                analytics.event({
                    type: EventType.DeleteIngestionSourceEvent,
                });
                message.success({ content: 'Removed ingestion source.', duration: 2 });
                const newRemovedUrns = [...removedUrns, urn];
                setRemovedUrns(newRemovedUrns);
                setTimeout(() => {
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
                    extraArgs: formatExtraArgs(recipeBuilderState.config?.extraArgs || []),
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

    const onChangeSort = (field, order) => {
        setSort(
            order
                ? {
                      sortOrder: order === 'ascend' ? SortOrder.Ascending : SortOrder.Descending,
                      field,
                  }
                : undefined,
        );
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
                        <Button
                            id={INGESTION_CREATE_SOURCE_ID}
                            type="text"
                            onClick={() => setIsBuildingSource(true)}
                            data-testid="create-ingestion-source-button"
                        >
                            <PlusOutlined /> Create new source
                        </Button>
                        <Button id={INGESTION_REFRESH_SOURCES_ID} type="text" onClick={onRefresh}>
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
                            onQueryChange={(q) => {
                                setPage(1);
                                debouncedSetQuery(q);
                            }}
                            entityRegistry={entityRegistry}
                            hideRecommendations
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
                    onChangeSort={onChangeSort}
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
                open={isBuildingSource}
                onSubmit={onSubmit}
                onCancel={onCancel}
            />
            {isViewingRecipe && <RecipeViewerModal recipe={focusSource?.config?.recipe} onCancel={onCancel} />}
            {focusExecutionUrn && (
                <ExecutionDetailsModal urn={focusExecutionUrn} open onClose={() => setFocusExecutionUrn(undefined)} />
            )}
        </>
    );
};
