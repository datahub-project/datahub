import React from 'react';
import styled from 'styled-components';

import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { SearchFilterLabel } from './SearchFilterLabel';

type Props = {
    facet: FacetMetadata;
    filter: FacetFilterInput;
    isCompact?: boolean;
};

const ValueFilterSection = styled.div<{ isCompact?: boolean }>`
    :hover {
        cursor: pointer;
    }
    margin: 4px;
    ${(props) => props.isCompact && 'display: inline;'}
`;

const StyledSearchFilterLabel = styled.div<{ isCompact?: boolean }>`
    margin: 4px;
    ${(props) => props.isCompact && 'display: inline;'}
`;

export const AdvancedSearchFilterValuesSection = ({ facet, filter, isCompact }: Props) => {
    return (
        <ValueFilterSection isCompact={isCompact}>
            {filter?.values?.map((value) => {
                const matchedAggregation = facet?.aggregations?.find((aggregation) => aggregation.value === value);
                return (
                    <StyledSearchFilterLabel key={value} isCompact={isCompact}>
                        <SearchFilterLabel
                            hideCount
                            field={filter.field}
                            value={value}
                            entity={matchedAggregation?.entity}
                        />
                    </StyledSearchFilterLabel>
                );
            })}
        </ValueFilterSection>
    );
};
