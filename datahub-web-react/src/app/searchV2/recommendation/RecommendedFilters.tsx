import React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata, FilterOperator } from '../../../types.generated';
import { FilterPill } from './FilterPill';
import { useGetRecommendedFilters } from './useGetRecommendedFilters';
import { RecommendedFilter } from './types';

const FilterPills = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    flex-shrink: 0;
    overflow-x: auto;
    /* Hide scrollbar for Chrome, Safari, and Opera */

    &::-webkit-scrollbar {
        display: none;
    }
`;

type Props = {
    availableFilters: FacetMetadata[];
    selectedFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
};

export const RecommendedFilters = ({ availableFilters, selectedFilters, onChangeFilters }: Props) => {
    const recommendedFilters = useGetRecommendedFilters(availableFilters, selectedFilters);

    if (!recommendedFilters.length) return null;

    const toggleFilter = (filter: RecommendedFilter) => {
        if (selectedFilters.find((f) => f.field === filter.field)) {
            // Remove the filter
            const newFilters = selectedFilters.filter((f) => f.field !== filter.field);
            onChangeFilters(newFilters);
        } else {
            // Add the filter
            const newFilters = [
                ...selectedFilters,
                {
                    field: filter.field,
                    values: [filter.value],
                    condition: FilterOperator.Equal,
                    negated: false,
                },
            ];
            onChangeFilters(newFilters);
        }
    };

    return (
        <FilterPills>
            {recommendedFilters.map((filter) => (
                <FilterPill filter={filter} onToggle={() => toggleFilter(filter)} />
            ))}
        </FilterPills>
    );
};
