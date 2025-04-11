import React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata, FilterOperator } from '../../../types.generated';
import { FilterPill } from './FilterPill';
import { useGetRecommendedFilters } from './useGetRecommendedFilters';
import { RecommendedFilter } from './types';

const FilterPills = styled.div`
    display: flex;
    flex-direction: row;
    margin-bottom: 8px;
    margin-left: 20px;
    padding-right: 20px;
    gap: 8px;
    flex-shrink: 0;
    overflow-x: auto;
    /* Hide scrollbar for Chrome, Safari, and Opera */

    &::-webkit-scrollbar {
        display: none;
    }

    mask-image: linear-gradient(to right, rgba(0, 0, 0, 1) 95%, rgba(255, 0, 0, 0.5) 100%, rgba(255, 0, 0, 0) 100%);
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
