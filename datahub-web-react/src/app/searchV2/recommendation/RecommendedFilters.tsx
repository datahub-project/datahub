/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { FilterPill } from '@app/searchV2/recommendation/FilterPill';
import { RecommendedFilter } from '@app/searchV2/recommendation/types';
import { useGetRecommendedFilters } from '@app/searchV2/recommendation/useGetRecommendedFilters';

import { FacetFilterInput, FacetMetadata, FilterOperator } from '@types';

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
