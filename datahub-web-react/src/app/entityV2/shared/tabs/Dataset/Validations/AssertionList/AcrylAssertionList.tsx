import { Empty } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SortingState } from '@components/components/Table/types';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { TableLoadingSkeleton } from '@app/entityV2/shared/TableLoadingSkeleton';
import { AcrylAssertionListFilters } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListFilters';
import { AcrylAssertionListTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListTable';
import { AssertionListTitleContainer } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AssertionListTitleContainer';
import { ASSERTION_DEFAULT_FILTERS } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/constant';
import { AssertionListFilter } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { useOpenAssertionDetailModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/hooks';
import { AssertionProfileDrawer } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileDrawer';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import {
    ASSERTION_CUSTOM_TYPE_FILTER_NAME,
    ASSERTION_FIELD_PATH_FILTER_NAME,
    ASSERTION_SOURCE_FILTER_NAME,
    ASSERTION_STATUS_FILTER_NAME,
    ASSERTION_TYPE_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    OWNERS_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import { useGetAssertionWithRunEventsQuery, useSearchAssertionsQuery } from '@src/graphql/assertion.generated';
import { useGetDatasetContractQuery } from '@src/graphql/contract.generated';
import {
    AndFilterInput,
    Assertion,
    AssertionResultType,
    AssertionSourceType,
    DataContract,
    EntityType,
    FacetFilterInput,
    FilterOperator,
    SortCriterion,
    SortOrder,
} from '@src/types.generated';

const AssertionListContainer = styled.div`
    display: flex;
    height: 100%;
    flex-direction: column;
    margin: 16px;
    flex: 1;
    overflow: hidden;
`;

const DEFAULT_ASSERTION_PAGE_SIZE = 25;
const DEFAULT_SORT_FIELD = 'lastCompletedTime';
const DEFAULT_SORT_ORDER = SortOrder.Descending;

const mapAssertionResultTypeToStatus = (status: AssertionResultType): string => {
    switch (status) {
        case AssertionResultType.Success:
            return 'PASSING';
        case AssertionResultType.Failure:
            return 'FAILING';
        case AssertionResultType.Error:
            return 'ERROR';
        case AssertionResultType.Init:
            return 'INIT';
        default:
            return status;
    }
};

export const convertSortFieldToQueryField = (field?: string) => {
    if (field === 'lastEvaluation') return DEFAULT_SORT_FIELD;
    if (field === 'type') return ASSERTION_TYPE_FILTER_NAME;
    return field || DEFAULT_SORT_FIELD;
};

export const buildAssertionSortCriteria = (field: string, sortOrder: SortOrder): SortCriterion[] => {
    const criteria = [{ field, sortOrder }];
    if (field === ASSERTION_TYPE_FILTER_NAME) {
        criteria.push({ field: ASSERTION_CUSTOM_TYPE_FILTER_NAME, sortOrder });
    }
    return criteria;
};

export const buildAssertionListFilters = (
    selectedFilters: AssertionListFilter,
    urnsToSearch: string[],
): AndFilterInput[] => {
    const filters: FacetFilterInput[] = [
        {
            field: LEGACY_ENTITY_FILTER_NAME,
            values: urnsToSearch,
            condition: FilterOperator.Equal,
        },
    ];
    const { status, type, source, tags, column, owners } = selectedFilters.filterCriteria;

    if (status.length) {
        filters.push({
            field: ASSERTION_STATUS_FILTER_NAME,
            values: status.map(mapAssertionResultTypeToStatus),
            condition: FilterOperator.Equal,
        });
    }
    if (type.length) {
        filters.push({ field: ASSERTION_TYPE_FILTER_NAME, values: type, condition: FilterOperator.Equal });
    }
    if (tags.length) {
        filters.push({ field: TAGS_FILTER_NAME, values: tags, condition: FilterOperator.Equal });
    }
    if (column.length) {
        filters.push({ field: ASSERTION_FIELD_PATH_FILTER_NAME, values: column, condition: FilterOperator.Equal });
    }
    if (owners.length) {
        filters.push({ field: OWNERS_FILTER_NAME, values: owners, condition: FilterOperator.Equal });
    }

    const includesExternal = source.includes(AssertionSourceType.External);
    const indexedSources = source.filter((value) => value !== AssertionSourceType.External);
    if (includesExternal && indexedSources.length === 0) {
        filters.push({
            field: ASSERTION_SOURCE_FILTER_NAME,
            values: [AssertionSourceType.Native, AssertionSourceType.Inferred],
            condition: FilterOperator.Equal,
            negated: true,
        });
    } else if (includesExternal && indexedSources.length === 1) {
        const excluded =
            indexedSources[0] === AssertionSourceType.Native
                ? AssertionSourceType.Inferred
                : AssertionSourceType.Native;
        filters.push({
            field: ASSERTION_SOURCE_FILTER_NAME,
            values: [excluded],
            condition: FilterOperator.Equal,
            negated: true,
        });
    } else if (!includesExternal && indexedSources.length) {
        filters.push({
            field: ASSERTION_SOURCE_FILTER_NAME,
            values: indexedSources,
            condition: FilterOperator.Equal,
        });
    }

    return [{ and: filters }];
};

/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertionList = () => {
    const { t } = useTranslation('entity.profile.validations');
    const { urn, entityData, loading: entityLoading } = useEntityData();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const [selectedFilters, setSelectedFilters] = useState<AssertionListFilter>(ASSERTION_DEFAULT_FILTERS);
    const [page, setPage] = useState(1);
    const [sortField, setSortField] = useState(DEFAULT_SORT_FIELD);
    const [sortOrder, setSortOrder] = useState<SortOrder>(DEFAULT_SORT_ORDER);
    const [focusedAssertionUrn, setFocusedAssertionUrn] = useState<string | null>(null);
    useOpenAssertionDetailModal(setFocusedAssertionUrn);

    const { data: focusedAssertionData, loading: focusedAssertionLoading } = useGetAssertionWithRunEventsQuery({
        variables: { assertionUrn: focusedAssertionUrn || '' },
        skip: !focusedAssertionUrn,
    });
    const focusedDatasetUrn = focusedAssertionData?.assertion?.dataset?.urn;
    const {
        data: contractData,
        loading: contractLoading,
        refetch: contractRefetch,
    } = useGetDatasetContractQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });
    const {
        data: focusedContractData,
        loading: focusedContractLoading,
        refetch: focusedContractRefetch,
    } = useGetDatasetContractQuery({
        variables: { urn: focusedDatasetUrn || urn },
        skip: !focusedAssertionUrn,
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        setPage(1);
    }, [selectedFilters]);

    const siblingUrns = entityData?.siblingsSearch?.searchResults?.map((result) => result.entity.urn) || [];
    const urnsToSearch = isHideSiblingMode ? [urn] : [urn, ...siblingUrns];
    const start = (page - 1) * DEFAULT_ASSERTION_PAGE_SIZE;
    const { searchText } = selectedFilters.filterCriteria;
    const { data, previousData, loading, refetch } = useSearchAssertionsQuery({
        variables: {
            input: {
                types: [EntityType.Assertion],
                query: searchText.trim() || '*',
                start,
                count: DEFAULT_ASSERTION_PAGE_SIZE,
                orFilters: buildAssertionListFilters(selectedFilters, urnsToSearch),
                sortInput: {
                    sortCriteria: buildAssertionSortCriteria(sortField, sortOrder),
                },
                searchFlags: { skipCache: true },
            },
            runEventsLimit: 1,
        },
        fetchPolicy: 'cache-and-network',
    });

    const activeData = data || previousData;
    const assertions = useMemo(
        () =>
            activeData?.searchAcrossEntities?.searchResults
                ?.map((result) => result.entity)
                .filter((entity) => entity.__typename === 'Assertion')
                .map((entity) => entity as Assertion) || [],
        [activeData],
    );
    const totalAssertions = activeData?.searchAcrossEntities?.total || 0;
    const facets = activeData?.searchAcrossEntities?.facets || undefined;
    const contract = contractData?.dataset?.contract as DataContract | undefined;
    const focusedContract = (focusedContractData?.dataset?.contract || contract) as DataContract | undefined;
    const hasRefinements =
        !!searchText.trim() ||
        Object.entries(selectedFilters.filterCriteria).some(
            ([key, value]) => key !== 'searchText' && Array.isArray(value) && value.length > 0,
        );

    const handleSortColumnChange = ({
        sortColumn,
        sortOrder: nextSortOrder,
    }: {
        sortColumn: string;
        sortOrder: SortingState;
    }) => {
        setSortField(convertSortFieldToQueryField(sortColumn));
        setSortOrder(nextSortOrder === SortingState.ASCENDING ? SortOrder.Ascending : SortOrder.Descending);
        setPage(1);
    };

    const renderContent = () => {
        if (entityLoading || contractLoading || (loading && !previousData)) {
            return <TableLoadingSkeleton />;
        }
        if (assertions.length) {
            return (
                <AcrylAssertionListTable
                    contract={contract}
                    assertions={assertions}
                    refetch={() => {
                        refetch();
                        contractRefetch();
                    }}
                    page={page}
                    setPage={setPage}
                    pageSize={DEFAULT_ASSERTION_PAGE_SIZE}
                    totalAssertions={totalAssertions}
                    loading={loading}
                    onSortColumnChange={handleSortColumnChange}
                    focusedAssertionUrn={focusedAssertionUrn}
                    onFocusAssertion={setFocusedAssertionUrn}
                />
            );
        }
        return (
            <Empty
                description={
                    hasRefinements ? t('assertionList.noAssertionsMatchFilters') : t('assertionList.noAssertionsRun')
                }
                image={Empty.PRESENTED_IMAGE_SIMPLE}
            />
        );
    };

    return (
        <AssertionListContainer>
            <AssertionListTitleContainer />
            <AcrylAssertionListFilters
                filteredAssertions={assertions}
                selectedFilters={selectedFilters}
                setSelectedFilters={setSelectedFilters}
                handleFilterChange={setSelectedFilters}
                totalAssertionCount={totalAssertions}
                facets={facets}
            />
            {renderContent()}
            {focusedAssertionUrn && (
                <AssertionProfileDrawer
                    urn={focusedAssertionUrn}
                    contract={focusedContract as DataContract}
                    contractLoading={focusedAssertionLoading || focusedContractLoading}
                    closeDrawer={() => setFocusedAssertionUrn(null)}
                    refetch={() => {
                        refetch();
                        contractRefetch();
                        focusedContractRefetch();
                    }}
                />
            )}
        </AssertionListContainer>
    );
};
