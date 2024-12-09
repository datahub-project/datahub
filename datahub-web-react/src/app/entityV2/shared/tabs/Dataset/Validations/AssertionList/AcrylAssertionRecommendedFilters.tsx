import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';

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
    height: 30px;
    font-size: 14px;
    border-radius: 20px;
    min-width: fit-content;
    cursor: pointer;
    color: ${({ selected }) => (selected ? REDESIGN_COLORS.WHITE : REDESIGN_COLORS.BODY_TEXT_GREY)};
    background-color: ${({ selected }) =>
        selected ? REDESIGN_COLORS.BACKGROUND_PRIMARY_1 : REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1};

    &:hover {
        background-color: ${REDESIGN_COLORS.BACKGROUND_PRIMARY_1};
        color: ${REDESIGN_COLORS.WHITE};
        box-shadow: none;
    }
    box-shadow: none;
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
            (filter: FilterItem) =>
                filter.category !== 'column' && (filter.count || transformedAppliedFilters.includes(filter.name)),
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
