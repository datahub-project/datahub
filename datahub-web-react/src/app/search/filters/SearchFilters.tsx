import React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import SearchFilter from './SearchFilter';

const SearchFiltersWrapper = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: 8px 24px;
`;

const FilterDropdownsWrapper = styled.div`
    display: flex;
`;

interface Props {
    availableFilters: FacetMetadata[] | null;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function SearchFilters({ availableFilters, activeFilters, onChangeFilters }: Props) {
    return (
        <SearchFiltersWrapper>
            <FilterDropdownsWrapper>
                {availableFilters?.map((filter) => (
                    <SearchFilter
                        key={filter.field}
                        filter={filter}
                        activeFilters={activeFilters}
                        onChangeFilters={onChangeFilters}
                    />
                ))}
            </FilterDropdownsWrapper>
        </SearchFiltersWrapper>
    );
}
