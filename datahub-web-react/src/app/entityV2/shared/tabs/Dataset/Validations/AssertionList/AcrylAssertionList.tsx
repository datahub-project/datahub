import { Empty, message } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { SortingState } from '@components/components/Table/types';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { AcrylAssertionListFilters } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListFilters';
import { AcrylAssertionListTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListTable';
import { AssertionListTitleContainer } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AssertionListTitleContainer';
import { ASSERTION_DEFAULT_FILTERS } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/constant';
import { AssertionListFilter } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { AssertionWithMonitorDetails } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { useOpenAssertionBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/hooks';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { LAST_ASSERTION_RUN_AT_SORT_FIELD } from '@app/observe/dataset/assertion/constants';
import {
    ASSERTION_SOURCE_FILTER_NAME,
    ASSERTION_STATUS_FILTER_NAME,
    ASSERTION_TYPE_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
} from '@app/searchV2/utils/constants';
// TODO: Define EMBEDDED_EXECUTOR_POOL_NAME constant or remove this check
// import { EMBEDDED_EXECUTOR_POOL_NAME } from '@app/shared/constants';
import { TableLoadingSkeleton } from '@src/app/entityV2/shared/TableLoadingSkeleton';
import { useGetDatasetContractQuery } from '@src/graphql/contract.generated';
import {
    AndFilterInput,
    Assertion,
    AssertionSourceType,
    DataContract,
    EntityType,
    FacetFilterInput,
    FilterOperator,
    SortOrder,
} from '@src/types.generated';

// TODO: Replace with correct query hook when available
// import { useIngestionSourceForEntityQuery } from '@graphql/ingestion.generated';
import { useSearchAssertionsQuery } from '@graphql/monitor.generated';

// TODO: Import AssertionMonitorBuilderDrawer when available
// import { AssertionMonitorBuilderDrawer } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/AssertionMonitorBuilderDrawer';

const AssertionListContainer = styled.div`
    display: flex;
    height: 100%;
    flex-direction: column;
    margin: 0px 20px;
    flex: 1;
    overflow: hidden;
`;

const DEFAULT_ASSERTION_PAGE_SIZE = 25;
const DEFAULT_SORT_ORDER = SortOrder.Descending;
const DEFAULT_SORT_FIELD = LAST_ASSERTION_RUN_AT_SORT_FIELD;

const convertSortFieldToQueryField = (field: string | undefined) => {
    if (!field) {
        return null;
    }
    switch (field) {
        case 'lastEvaluation':
            return LAST_ASSERTION_RUN_AT_SORT_FIELD;
        case 'type':
            return ASSERTION_TYPE_FILTER_NAME;
        default:
            return field;
    }
};

const buildOrFilters = (
    selectedFilters: AssertionListFilter,
    entityData: GenericEntityProperties | null,
    isHideSiblingMode: boolean,
    urn: string,
): AndFilterInput[] => {
    // First, choose which entities to search across
    const siblingUrns = entityData?.siblingsSearch?.searchResults.map((result) => result.entity.urn);
    // Include siblings if not in hide sibling mode
    const urnsToSearch = !isHideSiblingMode ? [urn, ...(siblingUrns || [])] : [urn];
    const filters: FacetFilterInput[] = [
        {
            field: LEGACY_ENTITY_FILTER_NAME,
            values: urnsToSearch,
            condition: FilterOperator.Equal,
        },
    ];

    // Next, add the filters
    const { status: statusFilters, type: typeFilters, source: sourceFilters } = selectedFilters.filterCriteria;
    if (statusFilters.length > 0) {
        filters.push({
            field: ASSERTION_STATUS_FILTER_NAME,
            values: statusFilters,
            condition: FilterOperator.Equal,
        });
    }

    if (typeFilters.length > 0) {
        filters.push({
            field: ASSERTION_TYPE_FILTER_NAME,
            values: typeFilters,
            condition: FilterOperator.Equal,
        });
    }

    if (sourceFilters.length > 0) {
        sourceFilters.forEach((source) => {
            if (source === AssertionSourceType.External) {
                // For external assertions, exclude native and inferred
                filters.push({
                    field: ASSERTION_SOURCE_FILTER_NAME,
                    values: [AssertionSourceType.Native, AssertionSourceType.Inferred],
                    condition: FilterOperator.In,
                    negated: true,
                });
            } else {
                filters.push({
                    field: ASSERTION_SOURCE_FILTER_NAME,
                    values: [source],
                });
            }
        });
    }

    return [
        {
            and: filters,
        },
    ];
};

/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertionList = () => {
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const [selectedFilters, setSelectedFilters] = useState<AssertionListFilter>(ASSERTION_DEFAULT_FILTERS);

    const [page, setPage] = useState(1);
    const [sortField, setSortField] = useState<string | null>(DEFAULT_SORT_FIELD);
    const [sortOrder, setSortOrder] = useState<SortOrder | null>(DEFAULT_SORT_ORDER);
    const { urn, entityData, loading: entityLoading } = useEntityData();
    // TODO: Replace with correct query hook when available
    const ingestionSourceData = null;
    const ingestionSourceLoading = false;
    // const { data: ingestionSourceData, loading: ingestionSourceLoading } = useIngestionSourceForEntityQuery({
    //     variables: { urn },
    //     fetchPolicy: 'cache-first',
    // });

    const {
        data: contractData,
        refetch: contractRefetch,
        loading: contractLoading,
    } = useGetDatasetContractQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    // Reset page to 1 when filters change
    useEffect(() => {
        setPage(1);
    }, [
        selectedFilters.filterCriteria.searchText,
        selectedFilters.filterCriteria.status,
        selectedFilters.filterCriteria.type,
        selectedFilters.filterCriteria.source,
    ]);

    useOpenAssertionBuilder(() => {
        if (!entityData?.platform) {
            message.open({
                content:
                    'Could not find platform details for this asset. Please contact support if this issue persists.',
                type: 'error',
            });
        }
    });

    const orFilters: AndFilterInput[] = buildOrFilters(selectedFilters, entityData, isHideSiblingMode, urn);
    const start = (page - 1) * DEFAULT_ASSERTION_PAGE_SIZE;
    const { searchText } = selectedFilters.filterCriteria;
    const {
        data: searchResults,
        refetch,
        loading: assertionLoading,
        previousData: previousSearchResults,
    } = useSearchAssertionsQuery({
        variables: {
            input: {
                types: [EntityType.Assertion],
                query: searchText || '*',
                start,
                count: DEFAULT_ASSERTION_PAGE_SIZE,
                orFilters,
                sortInput: {
                    sortCriterion: {
                        field: sortField || DEFAULT_SORT_FIELD,
                        sortOrder: sortOrder || DEFAULT_SORT_ORDER,
                    },
                },
            },
            runEventsLimit: 1, // Only need a single run event
        },
        fetchPolicy: 'cache-and-network',
    });

    const assertionMonitorData: AssertionWithMonitorDetails[] =
        searchResults?.searchAcrossEntities?.searchResults?.map((result) => {
            const assertion = result.entity as Assertion;
            // assertion.monitor is an EntityRelationshipsResult with a relationships array
            const relationshipsArray = ((assertion as any).monitor as any)?.relationships || [];
            return {
                ...assertion,
                monitors: relationshipsArray
                    .filter((r: any) => r.entity?.__typename === 'Monitor')
                    .map((r: any) => r.entity),
            } as AssertionWithMonitorDetails;
        }) ?? [];

    const prevAssertionMonitorData: AssertionWithMonitorDetails[] =
        previousSearchResults?.searchAcrossEntities?.searchResults?.map((result) => {
            const assertion = result.entity as Assertion;
            // assertion.monitor is an EntityRelationshipsResult with a relationships array
            const relationshipsArray = ((assertion as any).monitor as any)?.relationships || [];
            return {
                ...assertion,
                monitors: relationshipsArray
                    .filter((r: any) => r.entity?.__typename === 'Monitor')
                    .map((r: any) => r.entity),
            } as AssertionWithMonitorDetails;
        }) ?? [];

    const handleFilterChange = (filter: any) => {
        setSelectedFilters(filter);
    };

    const handleSortColumnChange = ({
        sortColumn,
        sortOrder: sortOrderParam,
    }: {
        sortColumn: string;
        sortOrder: SortingState;
    }) => {
        let newSortOrder: SortOrder | null = null;
        if (sortOrderParam === SortingState.ASCENDING) {
            newSortOrder = SortOrder.Ascending;
        } else if (sortOrderParam === SortingState.DESCENDING) {
            newSortOrder = SortOrder.Descending;
        } else {
            newSortOrder = null;
        }
        setSortField(convertSortFieldToQueryField(sortColumn));
        setSortOrder(newSortOrder);
    };

    const contract = contractData?.dataset?.contract as DataContract | undefined;
    const totalAssertions = searchResults?.searchAcrossEntities?.total ?? 0;
    const maybeExecutorId = (ingestionSourceData as any)?.ingestionSourceForEntity?.config?.executorId;
    const EMBEDDED_EXECUTOR_POOL_NAME = 'default'; // TODO: Define proper constant
    const isReachable = !maybeExecutorId || maybeExecutorId.toLowerCase().startsWith(EMBEDDED_EXECUTOR_POOL_NAME);
    const isLoading = entityLoading || assertionLoading || ingestionSourceLoading || contractLoading;
    const hasResults = assertionMonitorData.length > 0;
    const hasPreviousResults = prevAssertionMonitorData.length > 0;
    // To avoid the list jumping around, we will display the stale data while
    // refetching the new data
    const showPreviousResultsWhileRefetching = isLoading && hasPreviousResults;
    const results = !isLoading ? assertionMonitorData : prevAssertionMonitorData;
    const hasSearchQuery = searchText.trim() !== '';
    const { status: statusFilters, type: typeFilters, source: sourceFilters } = selectedFilters.filterCriteria;
    const hasActiveFilters = statusFilters.length > 0 || typeFilters.length > 0 || sourceFilters.length > 0;
    const hasUserAppliedRefinements = hasSearchQuery || hasActiveFilters;
    const refinementReturnedNoResults = !isLoading && !hasResults && hasUserAppliedRefinements;

    return (
        <AssertionListContainer>
            <AssertionListTitleContainer />
            <AcrylAssertionListFilters
                filteredAssertions={results as any}
                selectedFilters={selectedFilters}
                setSelectedFilters={setSelectedFilters}
                handleFilterChange={handleFilterChange}
                totalAssertionCount={totalAssertions}
                facets={searchResults?.searchAcrossEntities?.facets || undefined}
            />
            {isLoading && !hasPreviousResults ? <TableLoadingSkeleton /> : null}
            {!isLoading && !hasResults && !hasUserAppliedRefinements ? (
                <Empty description="No assertions have run" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : null}
            {refinementReturnedNoResults ? (
                <Empty
                    description="No assertions match your current filters and search criteria. Note that auto-generated names don't appear in search."
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                />
            ) : null}
            {(hasResults || showPreviousResultsWhileRefetching) && entityData ? (
                <AcrylAssertionListTable
                    contract={contract}
                    entityData={entityData}
                    assertions={hasResults ? assertionMonitorData : prevAssertionMonitorData}
                    refetch={() => {
                        refetch();
                        contractRefetch();
                    }}
                    isEntityReachable={isReachable}
                    page={page}
                    setPage={setPage}
                    pageSize={DEFAULT_ASSERTION_PAGE_SIZE}
                    totalAssertions={totalAssertions}
                    loading={showPreviousResultsWhileRefetching}
                    onSortColumnChange={handleSortColumnChange}
                />
            ) : null}
        </AssertionListContainer>
    );
};
