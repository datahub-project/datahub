import { SimpleSelect, Text, Tooltip } from '@components';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { InlineListSearch } from '@app/entityV2/shared/components/search/InlineListSearch';
import {
    DEFAULT_FILTER_OPTIONS,
    DEFAULT_STATUS_OPTIONS,
    FILTER_OPTIONS_DECODER,
    FILTER_OPTIONS_ENCODER,
    FilterOptions,
} from '@app/observe/dataset/assertion/AssertionsByTableSummary.utils';
import { AssertionsByTableSummaryTable } from '@app/observe/dataset/assertion/AssertionsByTableSummaryTable';
import {
    ASSERTIONS_DOCS_LINK,
    ASSERTION_RESULT_TYPE_OPTIONS_TO_FILTER_FIELD,
    AssertionResultTypeOptions,
    LAST_ASSERTION_RESULT_AT_SORT_FIELD,
} from '@app/observe/dataset/assertion/constants';
import { Header } from '@app/observe/dataset/shared/shared';
import { useSyncFiltersWithQueryParams } from '@app/observe/dataset/shared/util';
import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import {
    CONTAINER_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import {
    AndFilterInput,
    Dataset,
    Entity,
    EntityType,
    FacetFilterInput,
    FilterOperator,
    Maybe,
    SortOrder,
} from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: hidden;
`;

const FilterOptionsWrapper = styled.div`
    display: flex;
    gap: 12px;
    align-items: center;
`;

const EmptyStateContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 80%;
`;

type Props = {
    isAnomalyDetectionEnabled: boolean;
};

/**
 * A component which displays a summary of the datasets that are failing some assertions
 */
export const AssertionsByTableSummary = ({ isAnomalyDetectionEnabled }: Props) => {
    const userContext = useUserContext();
    const entityRegistry = useEntityRegistry();

    const tryGetDisplayName = (entity?: Maybe<Entity>): string | undefined => {
        if (!entity) {
            return undefined;
        }
        try {
            return entityRegistry.getDisplayName(entity.type, entity);
        } catch (error) {
            return undefined;
        }
    };
    const viewUrn = userContext.localState?.selectedViewUrn;

    const { getFilterFromQueryParams, setFilterToQueryParams } = useSyncFiltersWithQueryParams();

    // Filters state
    const [filterOptions, setFilterOptions] = useState<FilterOptions>(
        getFilterFromQueryParams(FILTER_OPTIONS_DECODER, DEFAULT_FILTER_OPTIONS),
    );
    useEffect(() => {
        setFilterToQueryParams(filterOptions, FILTER_OPTIONS_ENCODER);
    }, [filterOptions]); // eslint-disable-line react-hooks/exhaustive-deps

    // Pagination
    const { page, size } = filterOptions;
    const start = (page - 1) * size;
    const setPage = (newPage: number) => {
        setFilterOptions((options) => ({ ...options, page: newPage }));
    };
    const setPageSize = (newSize: number) => {
        setFilterOptions((options) => ({ ...options, size: newSize }));
    };

    // Filters decomposition
    const {
        query: searchQuery,
        statuses: selectedStatus,
        domains: selectedDomains,
        owners: selectedOwnership,
        platforms: selectedPlatforms,
        containers: selectedContainers,
        terms: selectedTerms,
        tags: selectedTags,
    } = filterOptions;
    const setSearchQuery = (query: string) => {
        setFilterOptions((options) => ({ ...options, query }));
    };
    const setSelectedStatus = (statuses: AssertionResultTypeOptions[]) => {
        setFilterOptions((options) => ({ ...options, statuses }));
    };
    const setSelectedDomains = (domains: string[]) => {
        setFilterOptions((options) => ({ ...options, domains }));
    };
    const setSelectedOwnership = (owners: string[]) => {
        setFilterOptions((options) => ({ ...options, owners }));
    };
    const setSelectedPlatforms = (platforms: string[]) => {
        setFilterOptions((options) => ({ ...options, platforms }));
    };
    const setSelectedContainers = (containers: string[]) => {
        setFilterOptions((options) => ({ ...options, containers }));
    };
    const setSelectedTerms = (terms: string[]) => {
        setFilterOptions((options) => ({ ...options, terms }));
    };
    const setSelectedTags = (tags: string[]) => {
        setFilterOptions((options) => ({ ...options, tags }));
    };

    // Has Filters
    const hasFilters =
        searchQuery.length > 0 ||
        selectedStatus.length !== DEFAULT_STATUS_OPTIONS.length ||
        selectedDomains.length > 0 ||
        selectedOwnership.length > 0 ||
        selectedPlatforms.length > 0 ||
        selectedContainers.length > 0 ||
        selectedTerms.length > 0 ||
        selectedTags.length > 0;

    // Reset page when filters change
    useEffect(
        () => {
            setPage(1);
        },
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [
            searchQuery,
            selectedStatus,
            selectedDomains,
            selectedOwnership,
            selectedPlatforms,
            selectedContainers,
            selectedTerms,
            selectedTags,
        ],
    );

    // Build orFilters
    const orFilters: AndFilterInput[] = [];
    selectedStatus.forEach((status) => {
        const andFilters: Array<FacetFilterInput> = [
            { field: ASSERTION_RESULT_TYPE_OPTIONS_TO_FILTER_FIELD[status], value: 'true' },
        ];

        if (selectedDomains.length > 0) {
            andFilters.push({ field: DOMAINS_FILTER_NAME, values: selectedDomains });
        }

        if (selectedOwnership.length > 0) {
            andFilters.push({ field: OWNERS_FILTER_NAME, values: selectedOwnership });
        }

        if (selectedPlatforms.length > 0) {
            andFilters.push({ field: PLATFORM_FILTER_NAME, values: selectedPlatforms });
        }

        if (selectedContainers.length > 0) {
            andFilters.push({ field: CONTAINER_FILTER_NAME, values: selectedContainers });
        }

        if (selectedTerms.length > 0) {
            andFilters.push({ field: GLOSSARY_TERMS_FILTER_NAME, values: selectedTerms });
        }

        if (selectedTags.length > 0) {
            andFilters.push({ field: TAGS_FILTER_NAME, values: selectedTags });
        }

        orFilters.push({ and: andFilters });
    });

    // Search
    const { data: searchResults, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.Dataset],
                query: searchQuery || '*',
                start,
                count: size,
                orFilters,
                viewUrn,
                sortInput: {
                    sortCriterion: {
                        field: LAST_ASSERTION_RESULT_AT_SORT_FIELD,
                        sortOrder: SortOrder.Descending,
                    },
                },
                searchFlags: {
                    skipCache: true,
                },
            },
        },
        fetchPolicy: 'cache-first',
    });

    // Search results decomposition
    const total = searchResults?.searchAcrossEntities?.total ?? 0;
    const facets = searchResults?.searchAcrossEntities?.facets;

    const datasets: Dataset[] =
        searchResults?.searchAcrossEntities?.searchResults?.map((result) => result.entity as Dataset) || [];

    if (total === 0 && !hasFilters && !loading) {
        return (
            <EmptyStateContainer>
                <Text size="lg" weight="semiBold">
                    Create Assertions to detect data quality issues.
                </Text>
                <Text size="lg" color="gray">
                    {isAnomalyDetectionEnabled ? (
                        <>
                            Tip: Use <i>&lsquo;Bulk Create&rsquo;</i> to set up AI Anomaly Detection.
                        </>
                    ) : (
                        'Be the first to know when your data breaks.'
                    )}
                </Text>
                <a href={ASSERTIONS_DOCS_LINK} target="_blank" rel="noreferrer">
                    <Text size="lg" weight="semiBold">
                        Learn more
                    </Text>
                </a>
            </EmptyStateContainer>
        );
    }

    return (
        <Container>
            <Header>
                {/* ************************* Search Component ************************* */}
                <InlineListSearch
                    inputTestId="embedded-search-bar"
                    searchText={searchQuery}
                    debouncedSetFilterText={(value) => {
                        setSearchQuery(value);
                        analytics.event({
                            type: EventType.DatasetHealthFilterEvent,
                            tabType: 'AssertionsByAsset',
                            filterType: 'search',
                            content: {
                                filterValue: value,
                            },
                        });
                    }}
                    matchResultCount={0}
                    numRows={0}
                    options={{
                        hideMatchCountText: true,
                        allowClear: true,
                    }}
                    entityTypeName="dataset"
                />

                {/* ************************* Filter Options ************************* */}
                {/* TODO: generalize the filter options so we don't have to copy and paste for each filter */}
                <FilterOptionsWrapper>
                    {viewUrn && (
                        <Tooltip title="You may change or remove the view via the search bar at the very top of the page.">
                            <Text color="primary" size="md">
                                *View is applied.
                            </Text>
                        </Tooltip>
                    )}
                    {/* ----------- Assertion status ----------- */}
                    <SimpleSelect
                        width="fit-content"
                        options={[
                            {
                                value: 'Failing',
                                label: 'Has a Failing assertion',
                            },
                            {
                                value: 'Passing',
                                label: 'Has a Passing assertion',
                            },
                            {
                                value: 'Error',
                                label: 'Has an Errored assertion',
                            },
                        ]}
                        initialValues={selectedStatus}
                        onUpdate={(values) => {
                            setSelectedStatus(values as AssertionResultTypeOptions[]);
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'AssertionsByAsset',
                                filterType: 'filter',
                                filterSubType: 'assertionStatus',
                                content: {
                                    filterValues: values,
                                },
                            });
                        }}
                        placeholder="Status"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Status',
                        }}
                        showClear={false}
                    />
                    {/* ----------- Domains ----------- */}
                    <BaseEntityFilter
                        entityTypes={[EntityType.Domain]}
                        renderEntity={(entity) => tryGetDisplayName(entity) || entity.urn}
                        filterName="Domain"
                        fieldName={DOMAINS_FILTER_NAME}
                        facetState={{ facet: facets?.find((facet) => facet.field === DOMAINS_FILTER_NAME) }}
                        appliedFilters={{
                            filters: [
                                {
                                    field: DOMAINS_FILTER_NAME,
                                    values: selectedDomains,
                                    condition: FilterOperator.In,
                                },
                            ],
                        }}
                        onUpdate={(values) => {
                            const selectedValues = values.filters?.[0]?.values ?? [];
                            setSelectedDomains(selectedValues);
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'AssertionsByAsset',
                                filterType: 'filter',
                                filterSubType: 'assetDomains',
                                content: {
                                    filterValues: selectedValues,
                                },
                            });
                        }}
                    />
                    {/* ----------- Owners ----------- */}
                    <BaseEntityFilter
                        entityTypes={[EntityType.CorpUser, EntityType.CorpGroup]}
                        renderEntity={(entity) => tryGetDisplayName(entity) || entity.urn}
                        filterName="Owner"
                        fieldName={OWNERS_FILTER_NAME}
                        facetState={{ facet: facets?.find((facet) => facet.field === OWNERS_FILTER_NAME) }}
                        appliedFilters={{
                            filters: [
                                {
                                    field: OWNERS_FILTER_NAME,
                                    values: selectedOwnership,
                                    condition: FilterOperator.In,
                                },
                            ],
                        }}
                        onUpdate={(values) => {
                            const selectedValues = values.filters?.[0]?.values ?? [];
                            setSelectedOwnership(selectedValues);
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'AssertionsByAsset',
                                filterType: 'filter',
                                filterSubType: 'assetOwners',
                                content: {
                                    filterValues: selectedValues,
                                },
                            });
                        }}
                    />
                    {/* ----------- Platforms ----------- */}
                    <BaseEntityFilter
                        entityTypes={[EntityType.DataPlatform]}
                        renderEntity={(entity) => tryGetDisplayName(entity) || entity.urn}
                        filterName="Platform"
                        fieldName={PLATFORM_FILTER_NAME}
                        facetState={{ facet: facets?.find((facet) => facet.field === PLATFORM_FILTER_NAME) }}
                        appliedFilters={{
                            filters: [
                                {
                                    field: PLATFORM_FILTER_NAME,
                                    values: selectedPlatforms,
                                    condition: FilterOperator.In,
                                },
                            ],
                        }}
                        onUpdate={(values) => {
                            const selectedValues = values.filters?.[0]?.values ?? [];
                            setSelectedPlatforms(selectedValues);
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'AssertionsByAsset',
                                filterType: 'filter',
                                filterSubType: 'assetPlatforms',
                                content: {
                                    filterValues: selectedValues,
                                },
                            });
                        }}
                    />

                    {/* ----------- Containers ----------- */}
                    <BaseEntityFilter
                        entityTypes={[EntityType.Container]}
                        renderEntity={(entity) => tryGetDisplayName(entity) || entity.urn}
                        filterName="Container"
                        fieldName={CONTAINER_FILTER_NAME}
                        facetState={{ facet: facets?.find((facet) => facet.field === CONTAINER_FILTER_NAME) }}
                        appliedFilters={{
                            filters: [
                                {
                                    field: CONTAINER_FILTER_NAME,
                                    values: selectedContainers,
                                    condition: FilterOperator.In,
                                },
                            ],
                        }}
                        onUpdate={(values) => {
                            const selectedValues = values.filters?.[0]?.values ?? [];
                            setSelectedContainers(selectedValues);
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'AssertionsByAsset',
                                filterType: 'filter',
                                filterSubType: 'assetContainers',
                                content: {
                                    filterValues: selectedValues,
                                },
                            });
                        }}
                    />

                    {/* ----------- Terms ----------- */}
                    <BaseEntityFilter
                        entityTypes={[EntityType.GlossaryTerm]}
                        renderEntity={(entity) => tryGetDisplayName(entity) || entity.urn}
                        filterName="Term"
                        fieldName={GLOSSARY_TERMS_FILTER_NAME}
                        facetState={{ facet: facets?.find((facet) => facet.field === GLOSSARY_TERMS_FILTER_NAME) }}
                        appliedFilters={{
                            filters: [
                                {
                                    field: GLOSSARY_TERMS_FILTER_NAME,
                                    values: selectedTerms,
                                    condition: FilterOperator.In,
                                },
                            ],
                        }}
                        onUpdate={(values) => {
                            const selectedValues = values.filters?.[0]?.values ?? [];
                            setSelectedTerms(selectedValues);
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'AssertionsByAsset',
                                filterType: 'filter',
                                filterSubType: 'assetTerms',
                                content: {
                                    filterValues: selectedValues,
                                },
                            });
                        }}
                    />

                    {/* ----------- Tags ----------- */}
                    <BaseEntityFilter
                        entityTypes={[EntityType.Tag]}
                        renderEntity={(entity) => tryGetDisplayName(entity) || entity.urn}
                        filterName="Tag"
                        fieldName={TAGS_FILTER_NAME}
                        facetState={{ facet: facets?.find((facet) => facet.field === TAGS_FILTER_NAME) }}
                        appliedFilters={{
                            filters: [
                                {
                                    field: TAGS_FILTER_NAME,
                                    values: selectedTags,
                                    condition: FilterOperator.In,
                                },
                            ],
                        }}
                        onUpdate={(values) => {
                            const selectedValues = values.filters?.[0]?.values ?? [];
                            setSelectedTags(selectedValues);
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'AssertionsByAsset',
                                filterType: 'filter',
                                filterSubType: 'assetTags',
                                content: {
                                    filterValues: selectedValues,
                                },
                            });
                        }}
                    />
                </FilterOptionsWrapper>
            </Header>
            {/* ************************* Render Table ************************* */}
            <AssertionsByTableSummaryTable
                datasets={datasets}
                isLoading={loading}
                page={page}
                setPage={setPage}
                pageSize={size}
                setPageSize={setPageSize}
                total={total}
            />
        </Container>
    );
};
