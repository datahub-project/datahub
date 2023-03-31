import React, { useState } from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { UnionType } from '../utils/constants';
import { hasAdvancedFilters } from '../utils/hasAdvancedFilters';
import AdvancedFilters from './AdvancedFilters';
import BasicFilters from './BasicFilters';

const SearchFiltersWrapper = styled.div<{ removePadding: boolean }>`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: ${(props) => (props.removePadding ? '8px 24px 4px 24px' : '8px 24px')};
`;

interface Props {
    availableFilters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onChangeUnionType: (unionType: UnionType) => void;
}

export default function SearchFilters({
    availableFilters,
    activeFilters,
    unionType,
    onChangeFilters,
    onChangeUnionType,
}: Props) {
    const onlyShowAdvancedFilters = hasAdvancedFilters(activeFilters, unionType);
    const [isShowingBasicFilters, setIsShowingBasicFilters] = useState(!onlyShowAdvancedFilters);

    return (
        <SearchFiltersWrapper removePadding={!isShowingBasicFilters}>
            {isShowingBasicFilters && (
                <BasicFilters
                    availableFilters={availableFilters}
                    activeFilters={activeFilters}
                    onChangeFilters={onChangeFilters}
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
