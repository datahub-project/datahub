import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import ActiveFilter from './ActiveFilter';
import SearchFilter from './SearchFilter';

const SearchFiltersWrapper = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: 8px 24px;
`;

const FlexWrapper = styled.div`
    display: flex;
    flex-wrap: wrap;
`;

const StyledDivider = styled(Divider)`
    margin: 8px 0 0 0;
`;

interface Props {
    availableFilters: FacetMetadata[] | null;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function SearchFilters({ availableFilters, activeFilters, onChangeFilters }: Props) {
    return (
        <SearchFiltersWrapper>
            <FlexWrapper>
                {availableFilters?.map((filter) => (
                    <SearchFilter
                        key={filter.field}
                        filter={filter}
                        activeFilters={activeFilters}
                        onChangeFilters={onChangeFilters}
                    />
                ))}
            </FlexWrapper>
            {activeFilters.length > 0 && (
                <>
                    <StyledDivider />
                    <FlexWrapper>
                        {activeFilters.map((activeFilter) => (
                            <ActiveFilter filter={activeFilter} availableFilters={availableFilters} />
                        ))}
                    </FlexWrapper>
                </>
            )}
        </SearchFiltersWrapper>
    );
}
