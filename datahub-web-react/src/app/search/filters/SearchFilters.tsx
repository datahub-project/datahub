import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { ORIGIN_FILTER_NAME } from '../utils/constants';
import ActiveFilter from './ActiveFilter';
import MoreFilters from './MoreFilters';
import SearchFilter from './SearchFilter';

const NUM_VISIBLE_FILTER_DROPDOWNS = 5;

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
    // only want Environment filter if there's 2 or more envs
    // TODO: sort on what we deem as priority order once we solidify that
    const filters = availableFilters?.filter((f) =>
        f.field === ORIGIN_FILTER_NAME ? f.aggregations.length >= 2 : true,
    );
    const visibleFilters = filters?.slice(0, NUM_VISIBLE_FILTER_DROPDOWNS);
    const hiddenFilters = filters?.slice(NUM_VISIBLE_FILTER_DROPDOWNS);

    return (
        <SearchFiltersWrapper>
            <FlexWrapper>
                {visibleFilters?.map((filter) => (
                    <SearchFilter
                        key={filter.field}
                        filter={filter}
                        activeFilters={activeFilters}
                        onChangeFilters={onChangeFilters}
                    />
                ))}
                {hiddenFilters && hiddenFilters.length > 0 && (
                    <MoreFilters
                        filters={hiddenFilters}
                        activeFilters={activeFilters}
                        onChangeFilters={onChangeFilters}
                    />
                )}
            </FlexWrapper>
            {activeFilters.length > 0 && (
                <>
                    <StyledDivider />
                    <FlexWrapper>
                        {activeFilters.map((activeFilter) => (
                            <ActiveFilter
                                key={activeFilter.field}
                                filter={activeFilter}
                                availableFilters={availableFilters}
                                activeFilters={activeFilters}
                                onChangeFilters={onChangeFilters}
                            />
                        ))}
                    </FlexWrapper>
                </>
            )}
        </SearchFiltersWrapper>
    );
}
