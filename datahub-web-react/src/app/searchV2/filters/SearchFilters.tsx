import React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { SEARCH_RESULTS_FILTERS_ID } from '../../onboarding/config/SearchOnboardingConfig';
import SearchFilterOptions from './SearchFilterOptions';
import SelectedSearchFilters from './SelectedSearchFilters';
import { UnionType } from '../utils/constants';

const Container = styled.div`
    background-color: #ffffff;
    border-radius: 8px;
    padding: 16px 24px 16px 32px;
    border: 1px solid #e8e8e8;
    box-shadow: 0px 4px 10px 0px #a8a8a840;
`;

const FilterSpacer = styled.div`
    margin: 16px 0px;
`;

interface Props {
    loading: boolean;
    availableFilters: FacetMetadata[] | null;
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onChangeUnionType: (unionType: UnionType) => void;
    onClearFilters: () => void;
}

export default function SearchFilters({
    loading,
    availableFilters,
    activeFilters,
    unionType,
    onChangeFilters,
    onChangeUnionType,
    onClearFilters,
}: Props) {
    return (
        <Container id={SEARCH_RESULTS_FILTERS_ID}>
            <SearchFilterOptions
                loading={loading}
                availableFilters={availableFilters}
                activeFilters={activeFilters}
                unionType={unionType}
                onChangeFilters={onChangeFilters}
            />
            {activeFilters.length > 0 && (
                <>
                    <FilterSpacer />
                    <SelectedSearchFilters
                        availableFilters={availableFilters}
                        selectedFilters={activeFilters}
                        unionType={unionType}
                        onChangeFilters={onChangeFilters}
                        onChangeUnionType={onChangeUnionType}
                        onClearFilters={onClearFilters}
                        showUnionType
                    />
                </>
            )}
        </Container>
    );
}
