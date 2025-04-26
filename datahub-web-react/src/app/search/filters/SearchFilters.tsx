import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { SEARCH_RESULTS_FILTERS_V2_INTRO } from '@app/onboarding/config/SearchOnboardingConfig';
import AdvancedFilters from '@app/search/filters/AdvancedFilters';
import BasicFilters from '@app/search/filters/BasicFilters';
import { FilterMode, FilterModes, UnionType } from '@app/search/utils/constants';

import { FacetFilterInput, FacetMetadata } from '@types';

const SearchFiltersWrapper = styled.div<{ removePadding: boolean }>`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: ${(props) => (props.removePadding ? '8px 24px 4px 24px' : '8px 24px')};
`;

interface Props {
    loading: boolean;
    mode: FilterMode;
    availableFilters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onClearFilters: () => void;
    onChangeUnionType: (unionType: UnionType) => void;
    onChangeMode: (mode: FilterMode) => void;
}

export default function SearchFilters({
    loading,
    mode,
    availableFilters,
    activeFilters,
    unionType,
    onChangeFilters,
    onClearFilters,
    onChangeUnionType,
    onChangeMode,
}: Props) {
    const [finalAvailableFilters, setFinalAvailableFilters] = useState(availableFilters);

    /**
     * Only update the active filters if we are done loading. Prevents jitter!
     */
    useEffect(() => {
        if (!loading && finalAvailableFilters !== availableFilters) {
            setFinalAvailableFilters(availableFilters);
        }
    }, [availableFilters, loading, finalAvailableFilters]);

    const isShowingBasicFilters = mode === FilterModes.BASIC;
    return (
        <SearchFiltersWrapper
            id={SEARCH_RESULTS_FILTERS_V2_INTRO}
            data-testid="search-filters-v2"
            removePadding={!isShowingBasicFilters && !!activeFilters.length}
        >
            {isShowingBasicFilters && (
                <BasicFilters
                    loading={loading}
                    availableFilters={finalAvailableFilters}
                    activeFilters={activeFilters}
                    onChangeFilters={onChangeFilters}
                    onClearFilters={onClearFilters}
                    showAdvancedFilters={() => onChangeMode(FilterModes.ADVANCED)}
                />
            )}
            {!isShowingBasicFilters && (
                <AdvancedFilters
                    availableFilters={finalAvailableFilters}
                    activeFilters={activeFilters}
                    unionType={unionType}
                    onChangeFilters={onChangeFilters}
                    onChangeUnionType={onChangeUnionType}
                    showBasicFilters={() => onChangeMode(FilterModes.BASIC)}
                />
            )}
        </SearchFiltersWrapper>
    );
}
