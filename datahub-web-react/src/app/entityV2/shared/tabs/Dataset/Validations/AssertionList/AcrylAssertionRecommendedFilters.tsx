import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { ANTD_GRAY, REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';

interface FilterItem {
    name: string;
    displayName: string;
    category: string;
    count: number;
}

interface AcrylAssertionRecommendedFiltersProps {
    filters: FilterItem[];
    appliedFilters: FilterItem[];
    onFilterChange: (updatedFilters: FilterItem[]) => void;
}

const FilterContainer = styled.div`
    display: flex;
    flex-direction: row;
    padding: 10px;
    gap: 10px;
    overflow: auto;
`;

const FilterItemRow = styled.div<{ selected: boolean }>`
    display: flex;
    align-items: center;
    gap: 4px;
    padding: 0 8px;
    height: 24px;
    font-size: 12px;
    border-radius: 20px;
    min-width: fit-content;
    cursor: pointer;
    color: ${({ selected }) => (selected ? REDESIGN_COLORS.WHITE : 'black')};
    background-color: ${({ selected }) => (selected ? REDESIGN_COLORS.BACKGROUND_PRIMARY_1 : ANTD_GRAY[3])};

    &:hover {
        background-color: ${REDESIGN_COLORS.BACKGROUND_PRIMARY_1};
        color: ${REDESIGN_COLORS.WHITE};
    }
`;

const FilterName = styled.span``;
const FilterCount = styled.span``;

export const AcrylAssertionRecommendedFilters: React.FC<AcrylAssertionRecommendedFiltersProps> = ({
    filters,
    appliedFilters,
    onFilterChange,
}) => {
    const [visibleFilters, setVisibleFilters] = useState<FilterItem[]>([]);
    const handleFilterClick = (filter: FilterItem) => {
        const isSelected = appliedFilters.some((appliedFilter) => appliedFilter.name === filter.name);
        const updatedFilters = isSelected
            ? appliedFilters.filter((appliedFilter) => appliedFilter.name !== filter.name)
            : [...appliedFilters, filter];

        onFilterChange(updatedFilters);
    };

    useEffect(() => {
        const transformedAppliedFilters = appliedFilters.map((filter) => filter.name);
        const newVisibleFilters = filters.filter(
            (filter: FilterItem) => filter.count || transformedAppliedFilters.includes(filter.name),
        );
        setVisibleFilters(newVisibleFilters);
    }, [filters, appliedFilters]);
    return (
        <FilterContainer>
            {visibleFilters.map((filter) => (
                <FilterItemRow
                    key={filter.name}
                    selected={appliedFilters.some((appliedFilter) => appliedFilter.name === filter.name)}
                    onClick={() => handleFilterClick(filter)}
                >
                    <FilterName>{filter.displayName}</FilterName>
                    <FilterCount>({filter.count})</FilterCount>
                </FilterItemRow>
            ))}
        </FilterContainer>
    );
};
