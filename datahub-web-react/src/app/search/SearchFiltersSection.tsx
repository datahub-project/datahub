import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { UnionType } from './utils/constants';
import { hasAdvancedFilters } from './utils/hasAdvancedFilters';
import { AdvancedSearchFilters } from './AdvancedSearchFilters';
import { SimpleSearchFilters } from './SimpleSearchFilters';

type Props = {
    filters?: Array<FacetMetadata> | null;
    selectedFilters: Array<FacetFilterInput>;
    unionType: UnionType;
    loading: boolean;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
    onChangeUnionType: (unionType: UnionType) => void;
};

const FiltersContainer = styled.div`
    display: block;
    max-width: 260px;
    min-width: 260px;
    overflow-wrap: break-word;
    border-right: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    max-height: 100%;
`;

const FiltersHeader = styled.div`
    font-size: 14px;
    font-weight: 600;

    padding-left: 20px;
    padding-right: 4px;
    padding-bottom: 8px;

    width: 100%;
    height: 47px;
    line-height: 47px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};

    justify-content: space-between;
    display: flex;
`;

const SearchFilterContainer = styled.div`
    padding-top: 10px;
`;

// This component renders the entire filters section that allows toggling
// between the simplified search experience and advanced search
export const SearchFiltersSection = ({
    filters,
    selectedFilters,
    unionType,
    loading,
    onChangeFilters,
    onChangeUnionType,
}: Props) => {
    const onlyShowAdvancedFilters = hasAdvancedFilters(selectedFilters, unionType);

    const [seeAdvancedFilters, setSeeAdvancedFilters] = useState(onlyShowAdvancedFilters);
    return (
        <FiltersContainer>
            <FiltersHeader>
                <span>Filter</span>
                <span>
                    <Button
                        disabled={onlyShowAdvancedFilters}
                        type="link"
                        onClick={() => setSeeAdvancedFilters(!seeAdvancedFilters)}
                    >
                        {seeAdvancedFilters ? 'Basic' : 'Advanced'}
                    </Button>
                </span>
            </FiltersHeader>
            {seeAdvancedFilters ? (
                <AdvancedSearchFilters
                    unionType={unionType}
                    selectedFilters={selectedFilters}
                    onFilterSelect={(newFilters) => onChangeFilters(newFilters)}
                    onChangeUnionType={onChangeUnionType}
                    facets={filters || []}
                    loading={loading}
                />
            ) : (
                <SearchFilterContainer>
                    <SimpleSearchFilters
                        loading={loading}
                        facets={filters || []}
                        selectedFilters={selectedFilters}
                        onFilterSelect={(newFilters) => onChangeFilters(newFilters)}
                    />
                </SearchFilterContainer>
            )}
        </FiltersContainer>
    );
};
