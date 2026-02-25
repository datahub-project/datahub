import { Checkbox, Loader, SearchBar, Text, colors } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { SelectedFilterValues } from '@app/entityV2/view/builder/SelectedFilterValues';
import { SELECTABLE_ASSET_ENTITY_TYPES } from '@app/entityV2/view/builder/constants';
import AssetFilters from '@app/homeV3/modules/assetCollection/AssetFilters';
import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { getEntityDisplayType } from '@app/searchV2/autoCompleteV2/utils';
import useAppliedFilters from '@app/searchV2/filtersV2/context/useAppliedFilters';
import { convertFiltersMapToFilters } from '@app/searchV2/filtersV2/utils';
import { UnionType } from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { Entity } from '@types';

const Container = styled.div`
    display: flex;
    width: 100%;
    gap: 8px;
`;

const LeftSection = styled.div`
    flex: 6;
    min-width: 0;
`;

const RightSection = styled.div`
    flex: 4;
    width: calc(40% - 20px);
`;

const VerticalDivider = styled.div`
    width: 1px;
    background-color: ${colors.gray[100]};
`;

const SearchHeader = styled(Text)`
    margin-bottom: 8px;
`;

const ResultsContainer = styled.div`
    margin: 0 -16px 0 -8px;
    position: relative;
    max-height: 300px;
    padding-right: 8px;
`;

const ScrollableResultsContainer = styled.div`
    max-height: inherit;
    overflow-y: auto;
`;

const LoaderContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px;
`;

const ItemDetailsContainer = styled.div`
    display: flex;
    align-items: center;
`;

const EmptyContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px;
`;

const ResultItemWrapper = styled.div`
    cursor: pointer;
`;

type Props = {
    selectedUrns: string[];
    onChangeSelectedUrns: (urns: string[]) => void;
};

export function SelectFilterValuesTab({ selectedUrns, onChangeSelectedUrns }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const [searchQuery, setSearchQuery] = useState<string | undefined>();
    const { appliedFilters, updateFieldFilters } = useAppliedFilters();

    const filters = useMemo(() => convertFiltersMapToFilters(appliedFilters), [appliedFilters]);
    const orFilters = generateOrFilters(UnionType.AND, filters);

    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: searchQuery || '*',
                start: 0,
                count: 20,
                types: SELECTABLE_ASSET_ENTITY_TYPES,
                orFilters,
                searchFlags: {
                    skipCache: true,
                },
            },
        },
    });

    const entities = useMemo(
        () =>
            data?.searchAcrossEntities?.searchResults
                ?.map((res) => res.entity)
                .filter((entity): entity is Entity => !!entity) || [],
        [data],
    );

    const handleCheckboxChange = useCallback(
        (urn: string) => {
            if (selectedUrns.includes(urn)) {
                onChangeSelectedUrns(selectedUrns.filter((u) => u !== urn));
            } else {
                onChangeSelectedUrns([...selectedUrns, urn]);
            }
        },
        [selectedUrns, onChangeSelectedUrns],
    );

    const handleRemoveUrn = useCallback(
        (urn: string) => {
            onChangeSelectedUrns(selectedUrns.filter((u) => u !== urn));
        },
        [selectedUrns, onChangeSelectedUrns],
    );

    const customDetailsRenderer = useCallback(
        (entity: Entity) => {
            const displayType = getEntityDisplayType(entity, entityRegistry);
            return (
                <ItemDetailsContainer>
                    <Text color="gray" size="sm">
                        {displayType}
                    </Text>
                    <Checkbox
                        size="xs"
                        isChecked={selectedUrns.includes(entity.urn)}
                        onCheckboxChange={() => handleCheckboxChange(entity.urn)}
                        data-testid="filter-value-selection-checkbox"
                    />
                </ItemDetailsContainer>
            );
        },
        [selectedUrns, handleCheckboxChange, entityRegistry],
    );

    const content = useMemo(() => {
        if (loading) {
            return (
                <LoaderContainer>
                    <Loader />
                </LoaderContainer>
            );
        }
        if (entities.length > 0) {
            return entities.map((entity) => (
                <ResultItemWrapper
                    key={entity.urn}
                    onClick={() => handleCheckboxChange(entity.urn)}
                    role="button"
                    tabIndex={0}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                            handleCheckboxChange(entity.urn);
                        }
                    }}
                >
                    <AutoCompleteEntityItem
                        entity={entity}
                        customDetailsRenderer={customDetailsRenderer}
                        padding="8px 0 8px 8px"
                    />
                </ResultItemWrapper>
            ));
        }
        return (
            <EmptyContainer>
                <Text color="gray">No results found.</Text>
            </EmptyContainer>
        );
    }, [loading, entities, handleCheckboxChange, customDetailsRenderer]);

    return (
        <Container>
            <LeftSection>
                <SearchHeader color="gray" weight="bold">
                    Search and Select Assets
                </SearchHeader>
                <SearchBar
                    value={searchQuery}
                    onChange={setSearchQuery}
                    placeholder="Search datasets, dashboards, domains..."
                />
                <AssetFilters
                    searchQuery={searchQuery}
                    appliedFilters={appliedFilters}
                    updateFieldFilters={updateFieldFilters}
                />
                <ResultsContainer>
                    <ScrollableResultsContainer data-testid="select-filter-values-search-results">
                        {content}
                    </ScrollableResultsContainer>
                </ResultsContainer>
            </LeftSection>
            <VerticalDivider />
            <RightSection>
                <SelectedFilterValues selectedUrns={selectedUrns} onRemoveUrn={handleRemoveUrn} />
            </RightSection>
        </Container>
    );
}
