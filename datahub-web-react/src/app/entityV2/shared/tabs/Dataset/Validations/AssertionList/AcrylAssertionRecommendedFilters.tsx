import { ANTD_GRAY, REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import React from 'react';
import styled from 'styled-components';

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
    color: ${({ selected }) => (selected ? REDESIGN_COLORS.WHITE : 'black')};
    cursor: pointer;
    gap: 4px;
    &:hover {
        background-color: ${REDESIGN_COLORS.BACKGROUND_PRIMARY_1};
        color: ${REDESIGN_COLORS.WHITE};
    }
    background-color: ${({ selected }) => (selected ? REDESIGN_COLORS.BACKGROUND_PRIMARY_1 : ANTD_GRAY[3])};
    height: 24px;
    padding-left: 8px;
    padding-right: 8px;
    border-radius: 20px;
    font-size: 12px;
    min-width: fit-content;
`;

const FilterName = styled.span``;

const FilterCount = styled.span``;

const AcrylAssertionRecommendedFilters: React.FC<AcrylAssertionRecommendedFiltersProps> = ({
    filters,
    appliedFilters,
    onFilterChange,
}) => {
    const handleFilterClick = (filter: FilterItem) => {
        const isSelected = appliedFilters.some((appliedFilter) => appliedFilter.name === filter.name);
        const updatedFilters = isSelected
            ? appliedFilters.filter((appliedFilter) => appliedFilter.name !== filter.name)
            : [...appliedFilters, filter];

        onFilterChange(updatedFilters);
    };

    return (
        <FilterContainer>
            {filters.map((filter) => (
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

export default AcrylAssertionRecommendedFilters;
