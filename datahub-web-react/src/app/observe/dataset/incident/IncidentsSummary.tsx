import { Text, Tooltip } from '@components';
import { Check } from 'phosphor-react';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { InlineListSearch } from '@app/entityV2/shared/components/search/InlineListSearch';
import { IncidentsSummaryTable } from '@app/observe/dataset/incident/IncidentsSummaryTable';
import { HAS_ACTIVE_INCIDENTS_FILTER_FIELD } from '@app/observe/dataset/incident/constants';
import { LAST_INCIDENT_CREATED_TIME_SORT_FIELD } from '@app/observe/dataset/incident/util';
import { Header } from '@app/observe/dataset/shared/shared';
import BaseEntityFilter from '@app/searchV2/filtersV2/filters/BaseEntityFilter/BaseEntityFilter';
import {
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { AndFilterInput, Dataset, Entity, EntityType, FilterOperator, Maybe, SortOrder } from '@types';

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

const DEFAULT_PAGE_SIZE = 10;
const INCIDENTS_DOCS_LINK = 'https://docs.datahub.com/docs/incidents/incidents';

/**
 * A component which displays a summary of the datasets that have active incidents globally
 */
export const IncidentsSummary = () => {
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

    const [page, setPage] = useState(1);
    const start = (page - 1) * DEFAULT_PAGE_SIZE;

    const [searchQuery, setSearchQuery] = useState('');
    const [selectedDomains, setSelectedDomains] = useState<string[]>([]);
    const [selectedOwnership, setSelectedOwnership] = useState<string[]>([]);
    const [selectedPlatforms, setSelectedPlatforms] = useState<string[]>([]);
    const [selectedTerms, setSelectedTerms] = useState<string[]>([]);
    const [selectedTags, setSelectedTags] = useState<string[]>([]);

    const hasFilters =
        searchQuery.length > 0 ||
        selectedDomains.length > 0 ||
        selectedOwnership.length > 0 ||
        selectedPlatforms.length > 0 ||
        selectedTerms.length > 0 ||
        selectedTags.length > 0;

    // Reset page when filters change
    useEffect(() => {
        setPage(1);
    }, [searchQuery, selectedDomains, selectedOwnership, selectedPlatforms, selectedTerms, selectedTags]);

    const orFilters: AndFilterInput[] = [{ and: [{ field: HAS_ACTIVE_INCIDENTS_FILTER_FIELD, value: 'true' }] }];

    if (selectedDomains.length > 0) {
        orFilters[0].and?.push({ field: DOMAINS_FILTER_NAME, values: selectedDomains });
    }

    if (selectedOwnership.length > 0) {
        orFilters[0].and?.push({ field: OWNERS_FILTER_NAME, values: selectedOwnership });
    }

    if (selectedPlatforms.length > 0) {
        orFilters[0].and?.push({ field: PLATFORM_FILTER_NAME, values: selectedPlatforms });
    }

    if (selectedTerms.length > 0) {
        orFilters[0].and?.push({ field: GLOSSARY_TERMS_FILTER_NAME, values: selectedTerms });
    }

    if (selectedTags.length > 0) {
        orFilters[0].and?.push({ field: TAGS_FILTER_NAME, values: selectedTags });
    }

    const { data: searchResults, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.Dataset],
                query: searchQuery || '*',
                start,
                count: DEFAULT_PAGE_SIZE,
                orFilters,
                viewUrn,
                sortInput: {
                    sortCriterion: {
                        field: LAST_INCIDENT_CREATED_TIME_SORT_FIELD,
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
    const datasets: Dataset[] =
        searchResults?.searchAcrossEntities?.searchResults?.map((result) => result.entity as Dataset) || [];

    if (total === 0 && !hasFilters && !loading) {
        return (
            <EmptyStateContainer>
                <Text size="xl" weight="semiBold" style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                    No active incidents <Check size={20} />
                </Text>
                <Text size="lg" color="gray">
                    Incidents help you track and resolve issues across your data landscape.
                </Text>
                <a href={INCIDENTS_DOCS_LINK} target="_blank" rel="noreferrer">
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
                            tabType: 'IncidentsByAsset',
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
                <FilterOptionsWrapper>
                    {viewUrn && (
                        <Tooltip title="You may change or remove the view via the search bar at the very top of the page.">
                            <Text color="primary" size="md">
                                *Global view is applied.
                            </Text>
                        </Tooltip>
                    )}
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
                                tabType: 'IncidentsByAsset',
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
                                tabType: 'IncidentsByAsset',
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
                                tabType: 'IncidentsByAsset',
                                filterType: 'filter',
                                filterSubType: 'assetPlatforms',
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
                                tabType: 'IncidentsByAsset',
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
                                tabType: 'IncidentsByAsset',
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
            <IncidentsSummaryTable
                datasets={datasets}
                isLoading={loading}
                page={page}
                setPage={setPage}
                pageSize={DEFAULT_PAGE_SIZE}
                total={total}
            />
        </Container>
    );
};
