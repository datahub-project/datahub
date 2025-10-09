import { SimpleSelect, Text, colors } from '@components';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { InlineListSearch } from '@app/entityV2/shared/components/search/InlineListSearch';
import {
    DEFAULT_FILTER_OPTIONS,
    DEFAULT_STAGE_OPTIONS,
    DEFAULT_STATUS_OPTIONS,
    FILTER_OPTIONS_DECODER,
    FILTER_OPTIONS_ENCODER,
    FilterOptions,
    INCIDENTS_DOCS_LINK,
    INCIDENT_STAGE_OPTIONS,
    INCIDENT_STAGE_OPTIONS_TO_LABEL,
    IncidentPriorityOptions,
    IncidentStatusOptions,
    IncidentWithRelationships,
    PRIORITY_OPTIONS,
    PRIORITY_OPTIONS_TO_LABEL,
    STATUS_OPTIONS,
    STATUS_OPTIONS_TO_LABEL,
    buildFilters,
} from '@app/observe/dataset/incident/IncidentsByIncidentSummary.utils';
import { IncidentsByIncidentSummaryTable } from '@app/observe/dataset/incident/IncidentsByIncidentSummaryTable';
import { INCIDENT_TYPE_OPTIONS } from '@app/observe/dataset/incident/constants';
import { Header } from '@app/observe/dataset/shared/shared';
import { useSyncFiltersWithQueryParams } from '@app/observe/dataset/shared/util';
import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import { TAGS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { Entity, EntityType, FilterOperator, IncidentStage, Maybe, SortOrder } from '@types';

const VerticalDivider = styled.div`
    width: 1px;
    height: 36px;
    background-color: ${colors.gray[200]};
`;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: hidden;
    height: 100%;
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

export const IncidentsByIncidentSummary = () => {
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

    const { getFilterFromQueryParams, setFilterToQueryParams } = useSyncFiltersWithQueryParams();

    const [filterOptions, setFilterOptions] = useState<FilterOptions>(
        getFilterFromQueryParams(FILTER_OPTIONS_DECODER, DEFAULT_FILTER_OPTIONS),
    );
    useEffect(() => {
        setFilterToQueryParams(filterOptions, FILTER_OPTIONS_ENCODER);
    }, [filterOptions]); // eslint-disable-line react-hooks/exhaustive-deps

    // pagination
    const { page, size } = filterOptions;

    const start = (page - 1) * size;
    const setPage = (newPage: number) => {
        setFilterOptions((options) => ({ ...options, page: newPage }));
    };
    const setPageSize = (newSize: number) => {
        setFilterOptions((options) => ({ ...options, size: newSize }));
    };

    // Incident filters
    const { statuses, priorities, types: incidentTypes, tags: incidentTags, stages: incidentStages } = filterOptions;
    const { query: searchQuery } = filterOptions;

    const setStatuses = (newStatuses: IncidentStatusOptions[]) => {
        setFilterOptions((options) => ({ ...options, statuses: newStatuses }));
    };
    const setIncidentStages = (newIncidentStages: IncidentStage[]) => {
        setFilterOptions((options) => ({ ...options, stages: newIncidentStages }));
    };
    const setPriorities = (newPriorities: IncidentPriorityOptions[]) => {
        setFilterOptions((options) => ({ ...options, priorities: newPriorities }));
    };
    const setSearchQuery = (query: string) => {
        setFilterOptions((options) => ({ ...options, query }));
    };
    const setIncidentTypes = (types: string[]) => {
        setFilterOptions((options) => ({ ...options, types }));
    };
    const setIncidentTags = (tags: string[]) => {
        setFilterOptions((options) => ({ ...options, tags }));
    };

    // Reset page when filters change
    useEffect(
        () => {
            setPage(1);
        },
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [searchQuery, statuses, incidentStages, priorities, incidentTypes, incidentTags],
    );

    // Has Filters
    const hasFilters =
        statuses.length !== DEFAULT_STATUS_OPTIONS.length ||
        incidentStages.length !== DEFAULT_STAGE_OPTIONS.length ||
        priorities.length !== PRIORITY_OPTIONS.length ||
        searchQuery.length > 0 ||
        incidentTypes.length !== INCIDENT_TYPE_OPTIONS.length ||
        incidentTags.length > 0;

    // Build filters for searching incidents directly
    const filters = buildFilters(statuses, incidentStages, priorities, incidentTypes, incidentTags);

    const { data: searchResults, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.Incident],
                query: searchQuery || '*',
                start,
                count: size,
                orFilters: [{ and: filters }],
                sortInput: {
                    sortCriterion: {
                        field: 'lastUpdated', // Sort by creation time
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

    const total = searchResults?.searchAcrossEntities?.total ?? 0;
    const facets = searchResults?.searchAcrossEntities?.facets;

    const incidents: IncidentWithRelationships[] =
        searchResults?.searchAcrossEntities?.searchResults?.map(
            (result) => result.entity as IncidentWithRelationships,
        ) || [];

    if (total === 0 && !hasFilters && !loading) {
        return (
            <EmptyStateContainer>
                <div>
                    <Text size="xl" weight="semiBold">
                        No incidents found
                    </Text>
                </div>
                <div>
                    <Text size="lg" color="gray">
                        Incidents help you track and resolve issues across your data landscape.
                    </Text>
                </div>
                <div>
                    <a href={INCIDENTS_DOCS_LINK} target="_blank" rel="noreferrer">
                        <Text size="lg" weight="semiBold">
                            Learn more
                        </Text>
                    </a>
                </div>
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
                            tabType: 'IncidentsByIncident',
                            filterType: 'search',
                            content: {
                                filterValue: value,
                            },
                        });
                    }}
                    matchResultCount={0}
                    numRows={0}
                    entityTypeName="incident"
                    options={{
                        hideMatchCountText: true,
                        allowClear: true,
                    }}
                />

                {/* ************************* Filter Options ************************* */}
                {/* TODO: generalize the filter options so we don't have to copy and paste for each filter */}
                <FilterOptionsWrapper>
                    {/* ************************* State Selector ************************* */}
                    <SimpleSelect
                        width="fit-content"
                        options={STATUS_OPTIONS.map((option) => ({
                            value: option,
                            label: STATUS_OPTIONS_TO_LABEL[option],
                        }))}
                        values={statuses}
                        onUpdate={(values) => {
                            setStatuses(values as IncidentStatusOptions[]);
                            if (values.length !== 0) {
                                analytics.event({
                                    type: EventType.DatasetHealthFilterEvent,
                                    tabType: 'IncidentsByIncident',
                                    filterType: 'filter',
                                    filterSubType: 'incidentStatus',
                                    content: {
                                        filterValues: values,
                                    },
                                });
                            }
                        }}
                        placeholder="Status"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Status',
                        }}
                        showClear
                    />
                    {/* ************************* Incident Stage Selector ************************* */}
                    <SimpleSelect
                        width="fit-content"
                        options={INCIDENT_STAGE_OPTIONS.map((option) => ({
                            value: option,
                            label: INCIDENT_STAGE_OPTIONS_TO_LABEL[option],
                        }))}
                        values={incidentStages}
                        onUpdate={(values) => {
                            setIncidentStages(values as IncidentStage[]);
                            if (values.length !== 0) {
                                analytics.event({
                                    type: EventType.DatasetHealthFilterEvent,
                                    tabType: 'IncidentsByIncident',
                                    filterType: 'filter',
                                    filterSubType: 'incidentStage',
                                    content: {
                                        filterValues: values,
                                    },
                                });
                            }
                        }}
                        placeholder="Stage"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Stage',
                        }}
                        showClear
                    />

                    {/* ************************* Priority Selector ************************* */}
                    <SimpleSelect
                        width="fit-content"
                        options={PRIORITY_OPTIONS.map((option) => ({
                            value: option,
                            label: PRIORITY_OPTIONS_TO_LABEL[option],
                        }))}
                        values={priorities}
                        onUpdate={(values) => {
                            setPriorities(values as IncidentPriorityOptions[]);
                            if (values.length !== 0) {
                                analytics.event({
                                    type: EventType.DatasetHealthFilterEvent,
                                    tabType: 'IncidentsByIncident',
                                    filterType: 'filter',
                                    filterSubType: 'incidentPriority',
                                    content: {
                                        filterValues: values,
                                    },
                                });
                            }
                        }}
                        placeholder="Priority"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Priority',
                        }}
                        showClear
                    />

                    {/* ************************* Incident filters divider ************************* */}
                    <VerticalDivider />

                    {/* ************************* Incident Type ************************* */}
                    <SimpleSelect
                        width="fit-content"
                        options={INCIDENT_TYPE_OPTIONS.map((option) => ({
                            value: option.value,
                            label: option.name,
                        }))}
                        values={incidentTypes}
                        onUpdate={(values) => {
                            setIncidentTypes(values);
                            if (values.length !== 0) {
                                analytics.event({
                                    type: EventType.DatasetHealthFilterEvent,
                                    tabType: 'IncidentsByIncident',
                                    filterType: 'filter',
                                    filterSubType: 'incidentType',
                                    content: {
                                        filterValues: values,
                                    },
                                });
                            }
                        }}
                        placeholder="Type"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Type',
                        }}
                        showClear
                    />

                    {/* ************************* Incident Tags ************************* */}
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
                                    values: incidentTags,
                                    condition: FilterOperator.In,
                                },
                            ],
                        }}
                        onUpdate={(values) => {
                            const selectedValues = values.filters?.[0]?.values ?? [];
                            setIncidentTags(selectedValues);
                            analytics.event({
                                type: EventType.DatasetHealthFilterEvent,
                                tabType: 'IncidentsByIncident',
                                filterType: 'filter',
                                filterSubType: 'incidentTags',
                                content: { filterValues: selectedValues },
                            });
                        }}
                    />
                </FilterOptionsWrapper>
            </Header>
            <IncidentsByIncidentSummaryTable
                incidents={incidents}
                total={total}
                loading={loading}
                page={page}
                setPage={setPage}
                pageSize={size}
                setPageSize={setPageSize}
                hasModifiedDefaultFilters={hasFilters}
            />
        </Container>
    );
};
