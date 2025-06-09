import { Pagination, SearchBar, SimpleSelect } from '@components';
import { InputRef, Modal, message } from 'antd';
import * as QueryString from 'query-string';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation } from 'react-router';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import EmptySources from '@app/ingestV2/EmptySources';
import { CLI_EXECUTOR_ID } from '@app/ingestV2/constants';
import { ExecutionDetailsModal } from '@app/ingestV2/executions/components/ExecutionDetailsModal';
import { isExecutionRequestActive } from '@app/ingestV2/executions/utils';
import RefreshButton from '@app/ingestV2/shared/components/RefreshButton';
import useCommandS from '@app/ingestV2/shared/hooks/useCommandS';
import useRefreshInterval from '@app/ingestV2/shared/hooks/useRefreshInterval';
import IngestionSourceTable from '@app/ingestV2/source/IngestionSourceTable';
import RecipeViewerModal from '@app/ingestV2/source/RecipeViewerModal';
import { IngestionSourceBuilderModal } from '@app/ingestV2/source/builder/IngestionSourceBuilderModal';
import { DEFAULT_EXECUTOR_ID, SourceBuilderState, StringMapEntryInput } from '@app/ingestV2/source/builder/types';
import {
    addToListIngestionSourcesCache,
    getIngestionSourceSystemFilter,
    getSortInput,
    removeFromListIngestionSourcesCache,
} from '@app/ingestV2/source/utils';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { INGESTION_REFRESH_SOURCES_ID } from '@app/onboarding/config/IngestionOnboardingConfig';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { PendingOwner } from '@app/sharedV2/owners/OwnersSection';

import {
    useCreateIngestionExecutionRequestMutation,
    useCreateIngestionSourceMutation,
    useDeleteIngestionSourceMutation,
    useGetIngestionSourceQuery,
    useListIngestionSourcesQuery,
    useUpdateIngestionSourceMutation,
} from '@graphql/ingestion.generated';
import { useBatchAddOwnersMutation } from '@graphql/mutations.generated';
import { IngestionSource, SortCriterion, UpdateIngestionSourceInput } from '@types';

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
    padding: 0 20px 16px 0;
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

interface Props {
    showCreateModal: boolean;
    setShowCreateModal: (show: boolean) => void;
    shouldPreserveParams: React.MutableRefObject<boolean>;
}

export const IngestionSourceList = ({ showCreateModal, setShowCreateModal, shouldPreserveParams }: Props) => {
    const location = useLocation();
    const me = useUserContext();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [searchInput, setSearchInput] = useState('');
    const searchInputRef = useRef<InputRef>(null);
    // highlight search input if user arrives with a query preset for salience
    useEffect(() => {
        if (paramsQuery?.length) {
            setQuery(paramsQuery);
            setSearchInput(paramsQuery);
            setTimeout(() => {
                searchInputRef.current?.focus?.();
            }, 0);
        }
    }, [paramsQuery]);

    const [page, setPage] = useState(1);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const [isViewingRecipe, setIsViewingRecipe] = useState<boolean>(false);
    const [focusSourceUrn, setFocusSourceUrn] = useState<undefined | string>(undefined);
    const [focusExecutionUrn, setFocusExecutionUrn] = useState<undefined | string>(undefined);

    // Set of removed urns used to account for eventual consistency
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);
    const [sourceFilter, setSourceFilter] = useState(IngestionSourceType.ALL);
    const [hideSystemSources, setHideSystemSources] = useState(true);
    const [sort, setSort] = useState<SortCriterion>();

    // Debounce the search query
    useDebounce(
        () => {
            setPage(1);
            setQuery(searchInput);
        },
        300,
        [searchInput],
    );

    // When source filter changes, reset page to 1
    useEffect(() => {
        setPage(1);
    }, [sourceFilter]);

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
    const filteredSources = useMemo(() => {
        const sources = data?.listIngestionSources?.ingestionSources || [];
        return sources.filter((source) => !removedUrns.includes(source.urn)) as IngestionSource[];
    }, [data?.listIngestionSources?.ingestionSources, removedUrns]);

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
    }, [refetch]);

    function hasActiveExecution() {
        return !!filteredSources.find((source) =>
            source.executions?.executionRequests?.find((request) => isExecutionRequestActive(request)),
        );
    }
    useRefreshInterval(onRefresh, hasActiveExecution);

    const executeIngestionSource = useCallback(
        (urn: string) => {
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
        },
        [createExecutionRequestMutation, onRefresh],
    );

    const onCreateOrUpdateIngestionSourceSuccess = () => {
        setTimeout(() => refetch(), 3000);
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
                    const ownersToAdd = owners?.filter((owner) => owner.ownerUrn !== me.urn);

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

                    if (ownersToAdd && ownersToAdd.length > 0) {
                        batchAddOwnersMutation({
                            variables: {
                                input: {
                                    owners: ownersToAdd,
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
                    setShowCreateModal(false);
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

    const deleteIngestionSource = useCallback(
        (urn: string) => {
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
                        message.error({
                            content: `Failed to remove ingestion source: \n ${e.message || ''}`,
                            duration: 3,
                        });
                    }
                });
        },
        [client, page, pageSize, query, refetch, removeIngestionSourceMutation, removedUrns],
    );

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

    const onEdit = useCallback(
        (urn: string) => {
            setShowCreateModal(true);
            setFocusSourceUrn(urn);
        },
        [setShowCreateModal],
    );

    const onView = useCallback((urn: string) => {
        setIsViewingRecipe(true);
        setFocusSourceUrn(urn);
    }, []);

    const onExecute = useCallback(
        (urn: string) => {
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
        },
        [executeIngestionSource],
    );

    const onDelete = useCallback(
        (urn: string) => {
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
        },
        [deleteIngestionSource],
    );

    const onCancel = () => {
        setShowCreateModal(false);
        setIsViewingRecipe(false);
        setFocusSourceUrn(undefined);
    };

    const onChangeSort = useCallback((field, order) => {
        setSort(getSortInput(field, order));
    }, []);

    const handleSetFocusExecutionUrn = useCallback((val) => setFocusExecutionUrn(val), []);

    return (
        <>
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
                                value={searchInput || ''}
                                onChange={(value) => setSearchInput(value)}
                                ref={searchInputRef}
                            />
                        </SearchContainer>
                        <FilterButtonsContainer>
                            <StyledSimpleSelect
                                options={[
                                    { label: 'All', value: '0' },
                                    { label: 'UI', value: '1' },
                                    { label: 'CLI', value: '2' },
                                ]}
                                values={[sourceFilter.toString()]}
                                onUpdate={(values) => setSourceFilter(Number(values[0]))}
                                showClear={false}
                                width="fit-content"
                            />
                            <RefreshButton onClick={onRefresh} id={INGESTION_REFRESH_SOURCES_ID} />
                        </FilterButtonsContainer>
                    </StyledTabToolbar>
                </HeaderContainer>
                {!loading && totalSources === 0 ? (
                    <EmptySources sourceType="sources" isEmptySearchResult={!!query} />
                ) : (
                    <>
                        <TableContainer>
                            <IngestionSourceTable
                                sources={filteredSources}
                                setFocusExecutionUrn={handleSetFocusExecutionUrn}
                                onExecute={onExecute}
                                onEdit={onEdit}
                                onView={onView}
                                onDelete={onDelete}
                                onChangeSort={onChangeSort}
                                isLoading={loading}
                                shouldPreserveParams={shouldPreserveParams}
                            />
                        </TableContainer>
                        <PaginationContainer>
                            <Pagination
                                currentPage={page}
                                itemsPerPage={pageSize}
                                totalPages={totalSources}
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
                initialState={removeExecutionsFromIngestionSource(focusSource)}
                open={showCreateModal}
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
