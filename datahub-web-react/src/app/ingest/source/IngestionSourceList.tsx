import React, { useCallback, useEffect, useState, useMemo } from 'react';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import { Button, SearchBar, SimpleSelect } from '@components';
import { OnboardingTour } from '@src/app/onboarding/OnboardingTour';
import { message, Modal, Pagination } from 'antd';
import styled from 'styled-components';
import { ArrowClockwise } from 'phosphor-react';
import { debounce } from 'lodash';
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
import { ExecutionDetailsModal } from './executions/ExecutionRequestDetailsModal';
import RecipeViewerModal from './RecipeViewerModal';
import IngestionSourceTable from './IngestionSourceTable';
import { scrollToTop } from '../../shared/searchUtils';
import useRefreshIngestionData from './executions/useRefreshIngestionData';
import { isExecutionRequestActive } from './executions/IngestionSourceExecutionList';
import analytics, { EventType } from '../../analytics';
import { INGESTION_REFRESH_SOURCES_ID } from '../../onboarding/config/IngestionOnboardingConfig';
import { useCommandS } from './hooks';

const PLACEHOLDER_URN = 'placeholder-urn';

const SourceContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
`;

const SourcePaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const StyledTabToolbar = styled(TabToolbar)`
    &&& {
        padding: 8px 0;
        height: auto;
        box-shadow: none;
    }
`;

const SearchContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    justify-content: space-between;
`;

const SearchAndFilterContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const RefreshButtonContainer = styled.div``;

const RotatingArrowClockwise = styled(ArrowClockwise)<{ $isRotating: boolean }>`
    transition: transform 0.5s ease;
    transform: rotate(${(props) => (props.$isRotating ? 360 : 0)}deg);
    ${(props) => !props.$isRotating && `transition: none;`}
`;

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
        };
    }
    return undefined;
};

interface IngestionSourceListProps {
    _onSwitchTab: (tab: string) => void;
    showCreateModal?: boolean;
    setShowCreateModal?: (show: boolean) => void;
}

const DEBOUNCE_MS = 250; // Reduced from 1000ms to 250ms for better responsiveness

export const IngestionSourceList = ({
    _onSwitchTab,
    showCreateModal,
    setShowCreateModal,
}: IngestionSourceListProps) => {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [searchText, setSearchText] = useState<string>('');
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [page, setPage] = useState(1);
    const [isBuildingSource, setIsBuildingSource] = useState<boolean>(false);
    const [isViewingRecipe, setIsViewingRecipe] = useState<boolean>(false);
    const [focusSourceUrn, setFocusSourceUrn] = useState<undefined | string>(undefined);
    const [focusExecutionUrn, setFocusExecutionUrn] = useState<undefined | string>(undefined);
    const [lastRefresh, setLastRefresh] = useState(0);
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);
    const [sourceFilter, setSourceFilter] = useState(IngestionSourceType.ALL);
    const [sort, setSort] = useState<SortCriterion>();
    const [hideSystemSources, setHideSystemSources] = useState(true);
    const [isRefreshing, setIsRefreshing] = useState(false);

    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const debouncedSetQuery = useCallback(
        (value: string) => {
            setQuery(value || undefined);
            setPage(1);
        },
        [setQuery, setPage],
    );

    const debouncedSearch = useMemo(() => debounce(debouncedSetQuery, DEBOUNCE_MS), [debouncedSetQuery]);

    // Update searchText when query param changes
    useEffect(() => {
        if (paramsQuery !== undefined) {
            setSearchText(paramsQuery);
        }
    }, [paramsQuery]);

    // Add effect to handle showCreateModal prop
    useEffect(() => {
        if (showCreateModal) {
            setIsBuildingSource(true);
        }
    }, [showCreateModal]);

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
                start: (page - 1) * DEFAULT_PAGE_SIZE,
                count: DEFAULT_PAGE_SIZE,
                query: query?.length ? query : undefined,
                filters: filters.length ? filters : undefined,
                sort,
            },
        },
        fetchPolicy: (query?.length || 0) > 0 ? 'no-cache' : 'cache-first',
    });

    const [createIngestionSource] = useCreateIngestionSourceMutation();
    const [updateIngestionSource] = useUpdateIngestionSourceMutation();
    const [createExecutionRequestMutation] = useCreateIngestionExecutionRequestMutation();
    const [removeIngestionSourceMutation] = useDeleteIngestionSourceMutation();

    const totalSources = data?.listIngestionSources?.total || 0;
    const sources = data?.listIngestionSources?.ingestionSources || [];
    const filteredSources = sources.filter((source) => !removedUrns.includes(source.urn)) as IngestionSource[];
    const focusSource =
        (focusSourceUrn && filteredSources.find((source) => source.urn === focusSourceUrn)) || undefined;

    const onRefresh = useCallback(() => {
        refetch();
        setLastRefresh(new Date().getTime());
    }, [refetch]);

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

    const onCancel = () => {
        setIsBuildingSource(false);
        setFocusSourceUrn(undefined);
        if (setShowCreateModal) {
            setShowCreateModal(false);
        }
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

                    // Update cache immediately
                    addToListIngestionSourcesCache(client, newSource, DEFAULT_PAGE_SIZE, query);

                    // Reset state and close modal
                    resetState();
                    setIsBuildingSource(false);
                    setFocusSourceUrn(undefined);
                    if (setShowCreateModal) {
                        setShowCreateModal(false);
                    }

                    // Show success message
                    message.success({
                        content: `Successfully created ingestion source!`,
                        duration: 3,
                    });

                    // Track analytics
                    analytics.event({
                        type: EventType.CreateIngestionSourceEvent,
                        sourceType: input.type,
                        interval: input.schedule?.interval,
                    });

                    // Execute if needed
                    if (shouldRun && result.data?.createIngestionSource) {
                        executeIngestionSource(result.data.createIngestionSource);
                    }

                    // Refresh data in background
                    refetch();
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
        removeFromListIngestionSourcesCache(client, urn, page, DEFAULT_PAGE_SIZE, query);
        removeIngestionSourceMutation({
            variables: { urn },
        })
            .then(() => {
                analytics.event({
                    type: EventType.DeleteIngestionSourceEvent,
                });
                message.success({
                    content: 'Removed ingestion source.',
                    duration: 2,
                });
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
        setFocusSourceUrn(urn);
        setIsViewingRecipe(true);
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
            zIndex: 1051,
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
            zIndex: 1051,
        });
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

    const onRefreshClick = useCallback(() => {
        setIsRefreshing(true);
        onRefresh();
        // Reset the rotation after animation completes
        setTimeout(() => setIsRefreshing(false), 500);
    }, [onRefresh]);

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading ingestion sources..." />}
            {error && (
                <Message type="error" content="Failed to load ingestion sources! An unexpected error occurred." />
            )}
            <SourceContainer>
                <OnboardingTour stepIds={[INGESTION_REFRESH_SOURCES_ID]} />
                <StyledTabToolbar>
                    <SearchContainer>
                        <SearchAndFilterContainer>
                            <StyledSearchBar
                                placeholder="Search..."
                                value={searchText}
                                onChange={(value) => {
                                    setSearchText(value);
                                    debouncedSearch(value);
                                }}
                                allowClear
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
                        </SearchAndFilterContainer>
                        <RefreshButtonContainer>
                            <Button id={INGESTION_REFRESH_SOURCES_ID} variant="text" onClick={onRefreshClick}>
                                <RotatingArrowClockwise $isRotating={isRefreshing} /> Refresh
                            </Button>
                        </RefreshButtonContainer>
                    </SearchContainer>
                </StyledTabToolbar>
                <IngestionSourceTable
                    sources={filteredSources || []}
                    lastRefresh={lastRefresh}
                    setFocusExecutionUrn={setFocusExecutionUrn}
                    onExecute={onExecute}
                    onEdit={onEdit}
                    onView={onView}
                    onDelete={onDelete}
                    onRefresh={onRefresh}
                    onChangeSort={onChangeSort}
                />
                <SourcePaginationContainer>
                    <StyledPagination
                        current={page}
                        pageSize={DEFAULT_PAGE_SIZE}
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
