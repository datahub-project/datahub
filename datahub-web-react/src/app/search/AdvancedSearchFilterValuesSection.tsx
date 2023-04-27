import React from 'react';
import styled from 'styled-components';

import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { SearchFilterLabel } from './SearchFilterLabel';

type Props = {
    facet: FacetMetadata;
    filter: FacetFilterInput;
};

const ValueFilterSection = styled.div`
    :hover {
        cursor: pointer;
    }
    margin: 4px;
`;

const StyledSearchFilterLabel = styled.div`
    margin: 4px;
`;

export const AdvancedSearchFilterValuesSection = ({ facet, filter }: Props) => {
    return (
        <ValueFilterSection>
            {filter?.values?.map((value) => {
                const matchedAggregation = facet?.aggregations?.find((aggregation) => aggregation.value === value);
                return (
                    <StyledSearchFilterLabel key={value}>
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
