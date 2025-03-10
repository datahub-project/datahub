import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { EntityType, FacetFilterInput, FacetMetadata } from '@src/types.generated';
import { convertToAvailableFilterPredictes } from '@src/app/searchV2/filters/utils';
import { FilterPredicate } from '@src/app/searchV2/filters/types';
import FiltersLoadingSection from '@src/app/searchV2/filters/SearchFiltersLoadingSection';
import Filter, { FilterLabels } from './Filter';

const Section = styled.div<{ removePadding?: boolean }>`
    margin-bottom: 10px;
    position: relative;
    height: 40px;
    display: flex;
`;

interface Props {
    name: string;
    loading: boolean;
    availableFilters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    aggregationsEntityTypes?: Array<EntityType>;
    customFilterLabels?: FilterLabels;
}

export default function FilterSection({
    name,
    loading,
    availableFilters,
    activeFilters,
    onChangeFilters,
    aggregationsEntityTypes,
    customFilterLabels,
}: Props) {
    const [finalAvailableFilters, setFinalAvailableFilters] = useState(availableFilters);

    useEffect(() => {
        if (!loading && finalAvailableFilters !== availableFilters) {
            setFinalAvailableFilters(availableFilters);
        }
    }, [availableFilters, loading, finalAvailableFilters]);

    const filterPredicates: FilterPredicate[] = convertToAvailableFilterPredictes(
        activeFilters,
        finalAvailableFilters || [],
    );

    return (
        <Section id={`${name}-filters-section`} data-testid={`${name}-filters-section`}>
            {loading && !finalAvailableFilters?.length && <FiltersLoadingSection />}
            {finalAvailableFilters?.map((filter) => (
                <Filter
                    key={filter.field}
                    filter={filter}
                    activeFilters={activeFilters}
                    onChangeFilters={onChangeFilters}
                    filterPredicates={filterPredicates}
                    customFilterLabels={customFilterLabels}
                    aggregationsEntityTypes={aggregationsEntityTypes}
                />
            ))}
        </Section>
    );
}
