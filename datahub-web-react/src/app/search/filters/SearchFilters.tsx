import React, { useState } from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { UnionType } from '../utils/constants';
import { hasAdvancedFilters } from '../utils/hasAdvancedFilters';
import AdvancedFilters from './AdvancedFilters';
import BasicFilters from './BasicFilters';
import { SEARCH_RESULTS_FILTERS_V2_INTRO } from '../../onboarding/config/SearchOnboardingConfig';

const SearchFiltersWrapper = styled.div<{ removePadding: boolean }>`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: ${(props) => (props.removePadding ? '8px 24px 4px 24px' : '8px 24px')};
`;

interface Props {
    availableFilters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onClearFilters: () => void;
    onChangeUnionType: (unionType: UnionType) => void;
}

export default function SearchFilters({
    availableFilters,
    activeFilters,
    unionType,
    onChangeFilters,
    onClearFilters,
    onChangeUnionType,
}: Props) {
    const onlyShowAdvancedFilters = hasAdvancedFilters(activeFilters, unionType);
    const [isShowingBasicFilters, setIsShowingBasicFilters] = useState(!onlyShowAdvancedFilters);

    return (
        <SearchFiltersWrapper
            id={SEARCH_RESULTS_FILTERS_V2_INTRO}
            data-testid="search-filters-v2"
            removePadding={!isShowingBasicFilters && !!activeFilters.length}
        >
            {isShowingBasicFilters && (
                <BasicFilters
                    availableFilters={availableFilters}
                    activeFilters={activeFilters}
                    onChangeFilters={onChangeFilters}
                    onClearFilters={onClearFilters}
                    showAdvancedFilters={() => setIsShowingBasicFilters(false)}
                />
            )}
            {!isShowingBasicFilters && (
                <AdvancedFilters
                    availableFilters={availableFilters}
                    activeFilters={activeFilters}
                    unionType={unionType}
                    onChangeFilters={onChangeFilters}
                    onChangeUnionType={onChangeUnionType}
                    showBasicFilters={() => setIsShowingBasicFilters(true)}
                    onlyShowAdvancedFilters={onlyShowAdvancedFilters}
                />
            )}
        </SearchFiltersWrapper>
    );
}
