import { Pagination, SearchBar, SimpleSelect } from '@components';
import { InputRef, message } from 'antd';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useHistory } from 'react-router';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import EmptySources from '@app/ingestV2/EmptySources';
import { CLI_EXECUTOR_ID } from '@app/ingestV2/constants';
import { ExecutionDetailsModal } from '@app/ingestV2/executions/components/ExecutionDetailsModal';
import CancelExecutionConfirmation from '@app/ingestV2/executions/components/columns/CancelExecutionConfirmation';
import useCancelExecution from '@app/ingestV2/executions/hooks/useCancelExecution';
import { ExecutionCancelInfo } from '@app/ingestV2/executions/types';
import { isExecutionRequestActive } from '@app/ingestV2/executions/utils';
import { useIngestionOnboardingRedesignV1 } from '@app/ingestV2/hooks/useIngestionOnboardingRedesignV1';
import RefreshButton from '@app/ingestV2/shared/components/RefreshButton';
import useCommandS from '@app/ingestV2/shared/hooks/useCommandS';
import IngestionSourceRefetcher from '@app/ingestV2/source/IngestionSourceRefetcher';
import IngestionSourceTable from '@app/ingestV2/source/IngestionSourceTable';
import RecipeViewerModal from '@app/ingestV2/source/RecipeViewerModal';
import { IngestionSourceBuilderModal } from '@app/ingestV2/source/builder/IngestionSourceBuilderModal';
import { DEFAULT_EXECUTOR_ID, SourceBuilderState, StringMapEntryInput } from '@app/ingestV2/source/builder/types';
import {
    addToListIngestionSourcesCache,
    removeFromListIngestionSourcesCache,
    updateListIngestionSourcesCache,
} from '@app/ingestV2/source/cacheUtils';
import {
    buildOwnerEntities,
    getIngestionSourceSystemFilter,
    getSortInput,
    mapSourceTypeAliases,
    removeExecutionsFromIngestionSource,
} from '@app/ingestV2/source/utils';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import { INGESTION_REFRESH_SOURCES_ID } from '@app/onboarding/config/IngestionOnboardingConfig';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useAddOwners } from '@app/sharedV2/owners/useAddOwners';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';
import { useUpdateOwners } from '@app/sharedV2/owners/useUpdateOwners';
import usePagination from '@app/sharedV2/pagination/usePagination';
import { PageRoutes } from '@conf/Global';

import {
    useCreateIngestionExecutionRequestMutation,
    useCreateIngestionSourceMutation,
    useDeleteIngestionSourceMutation,
    useListIngestionSourcesQuery,
    useUpdateIngestionSourceMutation,
} from '@graphql/ingestion.generated';
import { Entity, IngestionSource, Owner, SortCriterion, UpdateIngestionSourceInput } from '@types';

const PLACEHOLDER_URN = 'placeholder-urn';

const SourceContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: auto;
`;

const HeaderContainer = styled.div`
    flex-shrink: 0;
`;

const StyledTabToolbar = styled.div`
    display: flex;
    justify-content: space-between;
    padding: 1px 0 16px 0; // 1px at the top to prevent Select's border outline from cutting-off
    height: auto;
    z-index: unset;
    box-shadow: none;
    flex-shrink: 0;
`;

const SearchContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const FilterButtonsContainer = styled.div`
    display: flex;
    gap: 8px;
`;

const StyledSearchBar = styled(SearchBar)`
    width: 400px;
`;

const StyledSimpleSelect = styled(SimpleSelect)`
    display: flex;
    align-self: start;
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

export enum IngestionSourceType {
    ALL,
    UI,
    CLI,
}

const DEFAULT_PAGE_SIZE = 25;

interface Props {
    showCreateModal: boolean;
    setShowCreateModal: (show: boolean) => void;
    shouldPreserveParams: React.MutableRefObject<boolean>;
    hideSystemSources: boolean;
    setHideSystemSources: (show: boolean) => void;
    selectedTab: TabType | undefined | null;
    setSelectedTab: (selectedTab: TabType | null | undefined) => void;
    sourceFilter?: number;
    setSourceFilter: (sourceFilter: number | undefined) => void;
    searchQuery?: string;
    setSearchQuery: (query: string) => void;
}

export const IngestionSourceList = ({
    showCreateModal,
    setShowCreateModal,
    shouldPreserveParams,
    hideSystemSources,
    setHideSystemSources,
    selectedTab,
    setSelectedTab,
    sourceFilter: sourceFilterFromUrl,
    setSourceFilter: setSourceFilterFromUrl,
    searchQuery: searchQueryFromUrl,
    setSearchQuery: setSearchQueryFromUrl,
}: Props) => {
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [searchInput, setSearchInput] = useState('');
    const searchInputRef = useRef<InputRef>(null);

    const showIngestionOnboardingRedesignV1 = useIngestionOnboardingRedesignV1();

    const history = useHistory();

    // Initialize search input from URL parameter
    useEffect(() => {
        if (searchQueryFromUrl?.length) {
            setQuery(searchQueryFromUrl);
            setSearchInput(searchQueryFromUrl);
            setTimeout(() => {
                searchInputRef.current?.focus?.();
            }, 0);
        }
    }, [searchQueryFromUrl]);

    const handleSearchInputChange = (value: string) => {
        setSearchInput(value);
    };

    const { page, setPage, start, count: pageSize } = usePagination(DEFAULT_PAGE_SIZE);

    const [isViewingRecipe, setIsViewingRecipe] = useState<boolean>(false);
    const [focusSourceUrn, setFocusSourceUrn] = useState<undefined | string>(undefined);
    const [focusExecutionUrn, setFocusExecutionUrn] = useState<undefined | string>(undefined);
    const [sourcesToRefetch, setSourcesToRefetch] = useState<Set<string>>(new Set());
    const [executedUrns, setExecutedUrns] = useState<Set<string>>(new Set());
    const [finalSources, setFinalSources] = useState<IngestionSource[]>([]);
    const [totalSources, setTotalSources] = useState<number>(0);
    const [executionInfoToCancel, setExecutionInfoToCancel] = useState<ExecutionCancelInfo | undefined>();
    const [sourceUrnToExecute, setSourceUrnToExecute] = useState<string | null>();
    const [sourceUrnToDelete, setSourceUrnToDelete] = useState<string | null>(null);
    const [isModalWaiting, setIsModalWaiting] = useState<boolean>(false);

    // Set of removed urns used to account for eventual consistency
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);
    const [sort, setSort] = useState<SortCriterion>();

    const sourceFilter = sourceFilterFromUrl ?? IngestionSourceType.ALL;

    // Debounce the search query
    useDebounce(
        () => {
            setPage(1);
            setQuery(searchInput);
            setSearchQueryFromUrl(searchInput);
        },
        300,
        [searchInput],
    );

    // When source filter changes, reset page to 1
    useEffect(() => {
        setPage(1);
    }, [sourceFilter, setPage]);

    /**
     * Show or hide system ingestion sources using a hidden command S command.
     */
    useCommandS(() => setHideSystemSources(!hideSystemSources));

    // Ingestion Source Default Filters
    const filters = [getIngestionSourceSystemFilter(hideSystemSources)];
    if (sourceFilter !== IngestionSourceType.ALL) {
        filters.push({
            field: 'sourceExecutorId',
            values: [CLI_EXECUTOR_ID],
            negated: sourceFilter !== IngestionSourceType.CLI,
        });
    }

    const queryInputs = {
        start,
        count: pageSize,
        query: query?.length ? query : undefined,
        filters: filters.length ? filters : undefined,
        sort,
    };

    // Fetch list of Ingestion Sources
    const { loading, error, data, client, refetch } = useListIngestionSourcesQuery({
        variables: {
            input: queryInputs,
        },
        fetchPolicy: 'cache-and-network',
        nextFetchPolicy: 'cache-first',
    });

    const { defaultOwnershipType } = useOwnershipTypes();

    useEffect(() => {
        const sources = (data?.listIngestionSources?.ingestionSources || []) as IngestionSource[];
        setFinalSources(sources);
        setTotalSources(data?.listIngestionSources?.total || 0);
    }, [data?.listIngestionSources]);

    const [createIngestionSource] = useCreateIngestionSourceMutation();
    const [updateIngestionSource] = useUpdateIngestionSourceMutation();

    const addOwners = useAddOwners();
    const updateOwners = useUpdateOwners();

    // Execution Request queries
    const [createExecutionRequestMutation] = useCreateIngestionExecutionRequestMutation();
    const [removeIngestionSourceMutation] = useDeleteIngestionSourceMutation();

    const focusSource = finalSources.find((s) => s.urn === focusSourceUrn);
    const isLastPage = totalSources <= pageSize * page;
    // this is required when the ingestion source has not been created
    const [selectedSourceType, setSelectedSourceType] = useState<string | undefined>(undefined);

    useEffect(() => {
        const sources = (data?.listIngestionSources?.ingestionSources || []) as IngestionSource[];
        setFinalSources(sources);
        setTotalSources(data?.listIngestionSources?.total || 0);
    }, [data?.listIngestionSources]);

    useEffect(() => {
        setFinalSources((prev) => prev.filter((source) => !removedUrns.includes(source.urn)));
    }, [removedUrns]);

    // Add active sources to polling on initial load and refetch
    useEffect(() => {
        const activeSourceUrns = finalSources
            .filter((source) => source.executions?.executionRequests?.some(isExecutionRequestActive))
            .map((source) => source.urn);

        setSourcesToRefetch((prev) => new Set([...prev, ...activeSourceUrns]));
    }, [finalSources, refetch]);

    // Remove the sources from polling which are not displayed
    useEffect(() => {
        const displayedUrns = finalSources.map((source) => source.urn);
        setExecutedUrns((prev) => new Set([...prev].filter((urn) => displayedUrns.includes(urn))));
        setSourcesToRefetch((prev) => new Set([...prev].filter((urn) => displayedUrns.includes(urn))));
    }, [finalSources, setExecutedUrns, setSourcesToRefetch]);

    const executeIngestionSource = useCallback(
        (urn: string) => {
            setExecutedUrns((prev) => new Set(prev).add(urn));
            createExecutionRequestMutation({
                variables: {
                    input: { ingestionSourceUrn: urn },
                },
                refetchQueries: ['listIngestionExecutionRequests'],
            })
                .then(() => {
                    setSourcesToRefetch((prev) => new Set(prev).add(urn));
                    analytics.event({ type: EventType.ExecuteIngestionSourceEvent });
                    message.success({
                        content: `Successfully submitted ingestion execution request!`,
                        duration: 3,
                    });
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to submit ingestion execution request!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                    setExecutedUrns((prev) => {
                        const newSet = new Set(prev);
                        newSet.delete(urn);
                        return newSet;
                    });
                });
        },
        [createExecutionRequestMutation],
    );

    const onCreateOrUpdateIngestionSourceSuccess = () => {
        setShowCreateModal(false);
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
        existingOwners?: Owner[],
        owners?: Entity[],
    ) => {
        setIsModalWaiting(true);

        if (focusSourceUrn) {
            // Update
            updateIngestionSource({ variables: { urn: focusSourceUrn as string, input } })
                .then(() => {
                    updateOwners(owners, existingOwners, focusSourceUrn);

                    const updatedSource = {
                        config: {
                            ...input.config,
                            version: null,
                        },
                        name: input.name,
                        type: input.type,
                        schedule: input.schedule || null,
                        urn: focusSourceUrn,
                        ownership: {
                            owners: buildOwnerEntities(focusSourceUrn, owners, defaultOwnershipType) || [],
                        },
                    };
                    updateListIngestionSourcesCache(client, updatedSource, queryInputs, false);

                    analytics.event({
                        type: EventType.UpdateIngestionSourceEvent,
                        sourceType: input.type,
                        sourceUrn: focusSourceUrn,
                        interval: input.schedule?.interval,
                        numOwners: owners?.length,
                        outcome: shouldRun ? 'save_and_run' : 'save',
                    });
                    message.success({
                        content: `Successfully updated ingestion source!`,
                        duration: 3,
                    });
                    if (shouldRun) executeIngestionSource(focusSourceUrn);
                    else setSourcesToRefetch((prev) => new Set(prev).add(focusSourceUrn));

                    onCreateOrUpdateIngestionSourceSuccess();
                    resetState();
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to update ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                })
                .finally(() => {
                    setIsModalWaiting(false);
                });
        } else {
            // Create
            createIngestionSource({ variables: { input } })
                .then((result) => {
                    message.loading({ content: 'Loading...', duration: 2 });
                    const newUrn = result?.data?.createIngestionSource || PLACEHOLDER_URN;

                    const newSource: IngestionSource = {
                        urn: newUrn,
                        name: input.name,
                        type: input.type,
                        config: { executorId: '', recipe: '', version: null, debugMode: null, extraArgs: null },
                        schedule: {
                            interval: input.schedule?.interval || '',
                            timezone: input.schedule?.timezone || null,
                        },
                        platform: null,
                        executions: null,
                        source: input.source || null,
                        ownership: {
                            owners: buildOwnerEntities(newUrn, owners, defaultOwnershipType),
                            lastModified: {
                                time: 0,
                            },
                            __typename: 'Ownership' as const,
                        },
                        __typename: 'IngestionSource' as const,
                    };

                    addOwners(owners, newSource.urn);
                    addToListIngestionSourcesCache(client, newSource, queryInputs);
                    setFinalSources((currSources) => [newSource, ...currSources]);

                    analytics.event({
                        type: EventType.CreateIngestionSourceEvent,
                        sourceType: input.type,
                        sourceUrn: newSource.urn,
                        interval: input.schedule?.interval,
                        numOwners: owners?.length,
                        outcome: shouldRun ? 'save_and_run' : 'save',
                    });
                    message.success({
                        content: `Successfully created ingestion source!`,
                        duration: 3,
                    });
                    if (result.data?.createIngestionSource) {
                        if (shouldRun) {
                            executeIngestionSource(result.data.createIngestionSource);
                        } else setSourcesToRefetch((prev) => new Set(prev).add(newSource.urn));
                    }
                    onCreateOrUpdateIngestionSourceSuccess();
                    resetState();
                })
                .catch((e) => {
                    console.error(e);
                    message.destroy();
                    message.error({
                        content: `Failed to create ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                })
                .finally(() => {
                    setIsModalWaiting(false);
                });
        }
    };

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleConfirmDelete = useCallback(() => {
        if (!sourceUrnToDelete) return;

        removeFromListIngestionSourcesCache(client, sourceUrnToDelete, page, pageSize, query);
        removeIngestionSourceMutation({
            variables: { urn: sourceUrnToDelete },
        })
            .then(() => {
                analytics.event({
                    type: EventType.DeleteIngestionSourceEvent,
                });
                message.success({ content: 'Removed ingestion source.', duration: 2 });
                const newRemovedUrns = [...removedUrns, sourceUrnToDelete];
                setRemovedUrns(newRemovedUrns);
                setTimeout(() => {
                    refetch?.();
                }, 3000);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: `Failed to remove ingestion source: \n ${e.message || ''}`,
                        duration: 3,
                    });
                }
            })
            .finally(() => {
                setSourceUrnToDelete(null);
            });
    }, [client, page, pageSize, query, refetch, removeIngestionSourceMutation, removedUrns, sourceUrnToDelete]);

    const onSubmit = (recipeBuilderState: SourceBuilderState, resetState: () => void, shouldRun?: boolean) => {
        const existingOwners: Owner[] = focusSource?.ownership?.owners ?? [];

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
                // Preserve source field when editing existing sources (especially system sources)
                source: focusSource?.source
                    ? {
                          type: focusSource.source.type,
                      }
                    : undefined,
            },
            resetState,
            shouldRun,
            existingOwners,
            recipeBuilderState.owners,
        );
    };

    const onEdit = useCallback(
        (urn: string) => {
            if (showIngestionOnboardingRedesignV1) {
                history.push(PageRoutes.INGESTION_UPDATE.replace(':urn', urn));
            } else {
                setShowCreateModal(true);
                setFocusSourceUrn(urn);
            }
        },
        [setShowCreateModal, showIngestionOnboardingRedesignV1, history],
    );

    const onView = useCallback((urn: string) => {
        setIsViewingRecipe(true);
        setFocusSourceUrn(urn);
    }, []);

    const onExecute = useCallback((urn: string) => {
        setSourceUrnToExecute(urn);
    }, []);

    const handleConfirmExecute = useCallback(() => {
        if (sourceUrnToExecute) {
            executeIngestionSource(sourceUrnToExecute);
        }
        setSourceUrnToExecute(null);
    }, [sourceUrnToExecute, executeIngestionSource]);

    const onCancelExecution = useCallback((executionUrn: string | undefined, sourceUrn: string) => {
        if (!executionUrn) {
            console.error(`Can't cancel execution as it's urn is undefined. Source: ${sourceUrn}`);
            return;
        }

        setExecutionInfoToCancel({ executionUrn, sourceUrn });
    }, []);

    const cancelExecution = useCancelExecution();

    const onConfirmCancelExecution = useCallback(() => {
        if (executionInfoToCancel && executionInfoToCancel.executionUrn) {
            cancelExecution(executionInfoToCancel.executionUrn, executionInfoToCancel.sourceUrn);
            setSourcesToRefetch((prev) => new Set(prev).add(executionInfoToCancel.sourceUrn));
        }
        setExecutionInfoToCancel(undefined);
    }, [executionInfoToCancel, cancelExecution]);

    const onDelete = useCallback((urn: string) => {
        setSourceUrnToDelete(urn);
    }, []);

    const onCancel = () => {
        setShowCreateModal(false);
        setIsViewingRecipe(false);
        setFocusSourceUrn(undefined);
    };

    const onChangeSort = useCallback((field, order) => {
        setSort(getSortInput(field, order));
    }, []);

    const handleSetFocusExecutionUrn = useCallback(
        (val) => {
            if (showIngestionOnboardingRedesignV1) {
                history.push(PageRoutes.INGESTION_RUN_DETAILS.replace(':urn', val), {
                    fromUrl: tabUrlMap[TabType.Sources],
                });
            } else {
                setFocusExecutionUrn(val);
            }
        },
        [history, showIngestionOnboardingRedesignV1],
    );

    return (
        <>
            {error && (
                <Message type="error" content="Failed to load ingestion sources! An unexpected error occurred." />
            )}
            <SourceContainer>
                <HeaderContainer>
                    <StyledTabToolbar>
                        <SearchContainer>
                            <StyledSearchBar
                                placeholder="Search..."
                                value={searchInput || ''}
                                onChange={(value) => handleSearchInputChange(value)}
                                ref={searchInputRef}
                                data-testid="ingestion-sources-search"
                            />
                            <StyledSimpleSelect
                                options={[
                                    { label: 'All', value: '0' },
                                    { label: 'UI', value: '1' },
                                    { label: 'CLI', value: '2' },
                                ]}
                                values={[sourceFilter.toString()]}
                                onUpdate={(values) => setSourceFilterFromUrl(Number(values[0]))}
                                showClear={false}
                                width="fit-content"
                                size="lg"
                                data-testid="ingestions-type-filter"
                            />
                        </SearchContainer>
                        <FilterButtonsContainer>
                            <RefreshButton onClick={() => refetch()} id={INGESTION_REFRESH_SOURCES_ID} />
                        </FilterButtonsContainer>
                    </StyledTabToolbar>
                </HeaderContainer>
                {!loading && data?.listIngestionSources?.total === 0 ? (
                    <EmptySources sourceType="sources" isEmptySearchResult={!!query} />
                ) : (
                    <>
                        <TableContainer>
                            <IngestionSourceTable
                                sources={finalSources}
                                setFocusExecutionUrn={handleSetFocusExecutionUrn}
                                onExecute={onExecute}
                                onCancelExecution={onCancelExecution}
                                onEdit={onEdit}
                                onView={onView}
                                onDelete={onDelete}
                                onChangeSort={onChangeSort}
                                isLoading={
                                    loading && (!data || data?.listIngestionSources?.ingestionSources?.length === 0)
                                }
                                shouldPreserveParams={shouldPreserveParams}
                                isLastPage={isLastPage}
                                sourcesToRefetch={sourcesToRefetch}
                                executedUrns={executedUrns}
                                setSelectedTab={setSelectedTab}
                            />
                        </TableContainer>
                        <PaginationContainer>
                            <Pagination
                                currentPage={page}
                                itemsPerPage={pageSize}
                                total={totalSources}
                                showLessItems
                                onPageChange={onChangePage}
                                showSizeChanger={false}
                                hideOnSinglePage
                            />
                        </PaginationContainer>
                    </>
                )}
            </SourceContainer>
            <IngestionSourceBuilderModal
                initialState={mapSourceTypeAliases(removeExecutionsFromIngestionSource(focusSource))}
                open={showCreateModal}
                onSubmit={onSubmit}
                onCancel={onCancel}
                sourceRefetch={() => {
                    if (focusSource?.urn) {
                        setSourcesToRefetch((prev) => new Set(prev).add(focusSource.urn));
                    }
                    return Promise.resolve();
                }}
                selectedSource={focusSource}
                selectedSourceType={selectedSourceType}
                setSelectedSourceType={setSelectedSourceType}
                loading={isModalWaiting}
            />
            {isViewingRecipe && <RecipeViewerModal recipe={focusSource?.config?.recipe} onCancel={onCancel} />}
            {focusExecutionUrn && (
                <ExecutionDetailsModal urn={focusExecutionUrn} open onClose={() => setFocusExecutionUrn(undefined)} />
            )}
            <CancelExecutionConfirmation
                isOpen={!!executionInfoToCancel}
                onCancel={() => setExecutionInfoToCancel(undefined)}
                onConfirm={onConfirmCancelExecution}
            />
            <ConfirmationModal
                isOpen={!!sourceUrnToExecute}
                handleConfirm={handleConfirmExecute}
                handleClose={() => setSourceUrnToExecute(null)}
                modalTitle="Confirm Source Execution"
                modalText="Click 'Execute' to run this ingestion source."
                closeButtonText="Cancel"
                confirmButtonText="Execute"
            />
            <ConfirmationModal
                isOpen={!!sourceUrnToDelete}
                handleConfirm={handleConfirmDelete}
                handleClose={() => setSourceUrnToDelete(null)}
                modalTitle="Confirm Ingestion Source Removal"
                modalText="Are you sure you want to remove this ingestion source? Removing will terminate any scheduled ingestion runs."
                closeButtonText="Cancel"
                confirmButtonText="Yes"
            />
            {/* For refetching and polling */}
            {selectedTab === TabType.Sources &&
                Array.from(sourcesToRefetch).map((urn) => (
                    <IngestionSourceRefetcher
                        key={urn}
                        urn={urn}
                        setFinalSources={setFinalSources}
                        setSourcesToRefetch={setSourcesToRefetch}
                        setExecutedUrns={setExecutedUrns}
                        queryInputs={queryInputs}
                        isExecutedNow={executedUrns.has(urn)}
                    />
                ))}
        </>
    );
};
