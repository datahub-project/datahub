import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import Filter, { FilterLabels } from '@app/sharedV2/filters/Filter';
import FiltersLoadingSection from '@src/app/searchV2/filters/SearchFiltersLoadingSection';
import { FilterPredicate } from '@src/app/searchV2/filters/types';
import { convertToAvailableFilterPredictes } from '@src/app/searchV2/filters/utils';
import { EntityType, FacetFilterInput, FacetMetadata } from '@src/types.generated';

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
    noOfLoadingSkeletons?: number;
}

export default function FilterSection({
    name,
    loading,
    availableFilters,
    activeFilters,
    onChangeFilters,
    aggregationsEntityTypes,
    customFilterLabels,
    noOfLoadingSkeletons,
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
            {loading && !finalAvailableFilters?.length && (
                <FiltersLoadingSection noOfLoadingSkeletons={noOfLoadingSkeletons} />
            )}
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
