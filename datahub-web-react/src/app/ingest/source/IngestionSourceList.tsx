import { Button, SearchBar, SimpleSelect } from '@components';
import { Modal, Pagination, message } from 'antd';
import { ArrowClockwise } from 'phosphor-react';
import * as QueryString from 'query-string';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import { INGESTION_TAB_QUERY_PARAMS } from '@app/ingest/constants';
import IngestionSourceTable from '@app/ingest/source/IngestionSourceTable';
import RecipeViewerModal from '@app/ingest/source/RecipeViewerModal';
import { IngestionSourceBuilderModal } from '@app/ingest/source/builder/IngestionSourceBuilderModal';
import { DEFAULT_EXECUTOR_ID, SourceBuilderState, StringMapEntryInput } from '@app/ingest/source/builder/types';
import { ExecutionDetailsModal } from '@app/ingest/source/executions/ExecutionRequestDetailsModal';
import { isExecutionRequestActive } from '@app/ingest/source/executions/IngestionSourceExecutionList';
import useRefreshIngestionData from '@app/ingest/source/executions/useRefreshIngestionData';
import { useCommandS } from '@app/ingest/source/hooks';
import {
    CLI_EXECUTOR_ID,
    addToListIngestionSourcesCache,
    removeFromListIngestionSourcesCache,
} from '@app/ingest/source/utils';
import { INGESTION_REFRESH_SOURCES_ID } from '@app/onboarding/config/IngestionOnboardingConfig';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { PendingOwner } from '@app/sharedV2/owners/OwnersSection';
import { OnboardingTour } from '@src/app/onboarding/OnboardingTour';

import {
    useCreateIngestionExecutionRequestMutation,
    useCreateIngestionSourceMutation,
    useDeleteIngestionSourceMutation,
    useGetIngestionSourceQuery,
    useListIngestionSourcesQuery,
    useUpdateIngestionSourceMutation,
} from '@graphql/ingestion.generated';
import { useBatchAddOwnersMutation } from '@graphql/mutations.generated';
import { IngestionSource, SortCriterion, SortOrder, UpdateIngestionSourceInput } from '@types';

const PLACEHOLDER_URN = 'placeholder-urn';

const SourceContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
`;

const HeaderContainer = styled.div`
    flex-shrink: 0;
`;

const TableContainer = styled.div`
    flex: 1;
    overflow: auto;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    flex-shrink: 0;
`;

const StyledTabToolbar = styled(TabToolbar)`
    padding: 16px 20px;
    height: auto;
    z-index: unset;
    &&& {
        padding: 8px 20px;
        height: auto;
        box-shadow: none;
        flex-shrink: 0;
    }
`;

const SearchContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const RefreshButtonContainer = styled.div``;

const StyledSearchBar = styled(SearchBar)`
    width: 220px;
`;

const StyledPagination = styled(Pagination)`
    margin: 15px;
`;

const StyledSimpleSelect = styled(SimpleSelect)`
    display: flex;
    align-self: start;
`;

const SYSTEM_INTERNAL_SOURCE_TYPE = 'SYSTEM';

enum IngestionSourceType {
    ALL = 0,
    UI = 1,
    CLI = 2,
}

const DEFAULT_PAGE_SIZE = 25;

const removeExecutionsFromIngestionSource = (source) => {
    if (source) {
        return {
            name: source.name,
            type: source.type,
            schedule: source.schedule,
            config: source.config,
            owners: source.ownership?.owners,
        };
    }
    return undefined;
};

type Props = {
    showCreateModal?: boolean;
    setShowCreateModal?: (show: boolean) => void;
};

export const IngestionSourceList = ({ showCreateModal, setShowCreateModal }: Props) => {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.[INGESTION_TAB_QUERY_PARAMS.searchQuery] as string) || undefined;

    const [query, setQuery] = useState<undefined | string>(undefined);
    const searchInputRef = useRef<HTMLInputElement | null>(null);
    // highlight search input if user arrives with a query preset for salience
    useEffect(() => {
        if (paramsQuery?.length) {
            setQuery(paramsQuery);
            searchInputRef.current?.focus();
        }
    }, [paramsQuery]);

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

    // Add a useEffect to handle the showCreateModal prop
    useEffect(() => {
        if (showCreateModal && setShowCreateModal) {
            setIsBuildingSource(true);
            setShowCreateModal(false);
        }
    }, [showCreateModal, setShowCreateModal]);

    // When source filter changes, reset page to 1
    useEffect(() => {
        setPage(1);
    }, [sourceFilter]);

    /**
     * Show or hide system ingestion sources using a hidden command S command.
     */
    useCommandS(() => setHideSystemSources(!hideSystemSources));

    // Ingestion Source Default Filters
    const filters = hideSystemSources
        ? [{ field: 'sourceType', values: [SYSTEM_INTERNAL_SOURCE_TYPE], negated: true }]
        : [{ field: 'sourceType', values: [SYSTEM_INTERNAL_SOURCE_TYPE] }];

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
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();

    // Execution Request queries
    const [createExecutionRequestMutation] = useCreateIngestionExecutionRequestMutation();
    const [removeIngestionSourceMutation] = useDeleteIngestionSourceMutation();

    const totalSources = data?.listIngestionSources?.total || 0;
    const sources = data?.listIngestionSources?.ingestionSources || [];
    const filteredSources = sources.filter((source) => !removedUrns.includes(source.urn)) as IngestionSource[];
    const [focusSource, setFocusSource] = useState(
        (focusSourceUrn && filteredSources.find((source) => source.urn === focusSourceUrn)) || undefined,
    );

    const { data: focusSourceData, refetch: focusSourceRefetch } = useGetIngestionSourceQuery({
        variables: {
            urn: focusSourceUrn || '',
        },
        skip: !focusSourceUrn,
    });

    const combinedRefetch = async () => {
        await Promise.all([focusSourceRefetch(), refetch()]);
    };

    useEffect(() => {
        setFocusSource(focusSourceData?.ingestionSource as IngestionSource);
    }, [focusSourceData?.ingestionSource]);

    const onRefresh = useCallback(() => {
        refetch();
        // Used to force a re-render of the child execution request list.
        setLastRefresh(new Date().getTime());
    }, [refetch]);

    const handleSearch = (value: string) => {
        setPage(1);
        setQuery(value);
    };

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
        setTimeout(() => refetch(), 3000);
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
        owners?: PendingOwner[],
    ) => {
        if (focusSourceUrn) {
            // Update:
            updateIngestionSource({ variables: { urn: focusSourceUrn as string, input } })
                .then(() => {
                    if (owners && owners.length > 0) {
                        batchAddOwnersMutation({
                            variables: {
                                input: {
                                    owners: owners || [],
                                    resources: [{ resourceUrn: focusSourceUrn }],
                                },
                            },
                        });
                    }
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
                        ownership: null,
                    };
                    if (owners && owners.length > 0) {
                        batchAddOwnersMutation({
                            variables: {
                                input: {
                                    owners,
                                    resources: [{ resourceUrn: newSource.urn }],
                                },
                            },
                        });
                    }
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
            recipeBuilderState.owners,
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
            okButtonProps: { ['data-testid' as any]: 'confirm-delete-ingestion-source' },
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
                <OnboardingTour stepIds={[INGESTION_REFRESH_SOURCES_ID]} />
                <HeaderContainer>
                    <StyledTabToolbar>
                        <SearchContainer>
                            <StyledSearchBar
                                placeholder="Search..."
                                value={query || ''}
                                onChange={(value) => handleSearch(value)}
                            />
                            <StyledSimpleSelect
                                options={[
                                    { label: 'All', value: '0' },
                                    { label: 'UI', value: '1' },
                                    { label: 'CLI', value: '2' },
                                ]}
                                values={[sourceFilter.toString()]}
                                onUpdate={(values) => setSourceFilter(Number(values[0]))}
                                showClear={false}
                                width={60}
                            />
                        </SearchContainer>
                        <RefreshButtonContainer>
                            <Button id={INGESTION_REFRESH_SOURCES_ID} variant="text" onClick={onRefresh}>
                                <ArrowClockwise /> Refresh
                            </Button>
                        </RefreshButtonContainer>
                    </StyledTabToolbar>
                </HeaderContainer>
                <TableContainer>
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
                </TableContainer>
                <PaginationContainer>
                    <StyledPagination
                        current={page}
                        pageSize={pageSize}
                        total={totalSources}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </PaginationContainer>
            </SourceContainer>
            <IngestionSourceBuilderModal
                initialState={removeExecutionsFromIngestionSource(focusSource)}
                open={isBuildingSource}
                onSubmit={onSubmit}
                onCancel={onCancel}
                sourceRefetch={combinedRefetch}
                selectedSource={focusSource}
            />
            {isViewingRecipe && <RecipeViewerModal recipe={focusSource?.config?.recipe} onCancel={onCancel} />}
            {focusExecutionUrn && (
                <ExecutionDetailsModal urn={focusExecutionUrn} open onClose={() => setFocusExecutionUrn(undefined)} />
            )}
        </>
    );
};
