import { Select, Text } from '@components';
import { Check } from 'phosphor-react';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { InlineListSearch } from '@app/entityV2/shared/components/search/InlineListSearch';
import { IncidentsSummaryTable } from '@app/observe/dataset/incident/IncidentsSummaryTable';
import { HAS_ACTIVE_INCIDENTS_FILTER_FIELD } from '@app/observe/dataset/incident/constants';
import { LAST_INCIDENT_CREATED_TIME_SORT_FIELD } from '@app/observe/dataset/incident/util';
import { Header } from '@app/observe/dataset/shared/shared';
import {
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { FacetFieldsFragment, useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import {
    AndFilterInput,
    DataPlatform,
    Dataset,
    Domain,
    EntityType,
    GlossaryTerm,
    OwnerType,
    SortOrder,
    Tag,
} from '@types';

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

const getSelectOptionsForField = (
    field: string,
    facets: FacetFieldsFragment[],
    nameExtractor: (entity) => string | undefined,
    getIcon?: (entity) => React.ReactNode,
) => {
    return (
        facets
            ?.find((facet) => facet.field === field)
            ?.aggregations.map((agg) => {
                const name = nameExtractor(agg.entity) || agg.value;
                const icon = getIcon?.(agg.entity);
                return {
                    value: agg.value,
                    label: name,
                    icon,
                };
            }) || []
    );
};

/**
 * A component which displays a summary of the datasets that have active incidents globally
 */
export const IncidentsSummary = () => {
    const userContext = useUserContext();
    const entityRegistry = useEntityRegistry();
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

    if (total === 0 && !hasFilters) {
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
                    debouncedSetFilterText={(event) => setSearchQuery(event.target.value)}
                    matchResultCount={0}
                    numRows={0}
                    options={{
                        hideMatchCountText: true,
                    }}
                    entityTypeName="dataset"
                />

                {/* ************************* Filter Options ************************* */}
                <FilterOptionsWrapper>
                    {/* ----------- Domains ----------- */}
                    <Select
                        width="fit-content"
                        options={getSelectOptionsForField(
                            DOMAINS_FILTER_NAME,
                            facets || [],
                            (entity) => (entity as Domain).properties?.name,
                        )}
                        values={selectedDomains}
                        onUpdate={(values) => setSelectedDomains(values as string[])}
                        placeholder="Domains"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Domains',
                        }}
                        showClear
                        emptyState={<Text color="gray">No results have Domains.</Text>}
                    />
                    {/* ----------- Owners ----------- */}
                    <Select
                        width="fit-content"
                        options={getSelectOptionsForField(OWNERS_FILTER_NAME, facets || [], (entity: OwnerType) => {
                            try {
                                if (entity.type === EntityType.CorpUser) {
                                    return entityRegistry.getDisplayName(EntityType.CorpUser, entity);
                                }
                                return entityRegistry.getDisplayName(EntityType.CorpGroup, entity);
                            } catch (error) {
                                return entity.urn;
                            }
                        })}
                        values={selectedOwnership}
                        onUpdate={(values) => setSelectedOwnership(values as string[])}
                        placeholder="Owners"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Owners',
                        }}
                        showClear
                        emptyState={<Text color="gray">No results have Owners.</Text>}
                    />
                    {/* ----------- Platforms ----------- */}
                    <Select
                        width="fit-content"
                        options={getSelectOptionsForField(
                            PLATFORM_FILTER_NAME,
                            facets || [],
                            (entity: DataPlatform) => entityRegistry.getDisplayName(EntityType.DataPlatform, entity),
                            (entity: DataPlatform) => (
                                <PlatformIcon platform={entity} />
                            ),
                        )}
                        values={selectedPlatforms}
                        onUpdate={(values) => setSelectedPlatforms(values as string[])}
                        placeholder="Platforms"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Platforms',
                        }}
                        showClear
                        emptyState={<Text color="gray">No results have Platforms.</Text>}
                    />

                    {/* ----------- Terms ----------- */}
                    <Select
                        width="fit-content"
                        options={getSelectOptionsForField(
                            GLOSSARY_TERMS_FILTER_NAME,
                            facets || [],
                            (entity: GlossaryTerm) => entityRegistry.getDisplayName(EntityType.GlossaryTerm, entity),
                        )}
                        values={selectedTerms}
                        onUpdate={(values) => setSelectedTerms(values as string[])}
                        placeholder="Terms"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Terms',
                        }}
                        showClear
                        emptyState={<Text color="gray">No results have Terms.</Text>}
                    />

                    {/* ----------- Tags ----------- */}
                    <Select
                        width="fit-content"
                        options={getSelectOptionsForField(TAGS_FILTER_NAME, facets || [], (entity: Tag) =>
                            entityRegistry.getDisplayName(EntityType.Tag, entity),
                        )}
                        values={selectedTags}
                        onUpdate={(values) => setSelectedTags(values as string[])}
                        placeholder="Tags"
                        isMultiSelect
                        selectLabelProps={{
                            variant: 'labeled',
                            label: 'Tags',
                        }}
                        showClear
                        emptyState={<Text color="gray">No results have Tags.</Text>}
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
