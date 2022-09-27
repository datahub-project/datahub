import React from 'react';
import styled from 'styled-components';

import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { SearchFilterLabel } from './SearchFilterLabel';

type Props = {
    facet: FacetMetadata;
    filter: FacetFilterInput;
};

const ValueFilterSection = styled.div`
    :hover {
        cursor: pointer;
    }
    border-top: 1px solid ${ANTD_GRAY[3]};
`;

const StyledSearchFilterLabel = styled.div`
    margin: 4px;
`;

export const AdvancedSearchFilterValuesSection = ({ facet, filter }: Props) => {
    return (
        <ValueFilterSection>
            {filter?.values?.map((value) => {
                const matchedAggregation = facet?.aggregations?.find((aggregation) => aggregation.value === value);
                if (!matchedAggregation) return <span>{value}</span>;

                return (
                    <StyledSearchFilterLabel>
                        <SearchFilterLabel hideCount aggregation={matchedAggregation} field={value} />
                    </StyledSearchFilterLabel>
                );
            })}
        </ValueFilterSection>
    );
};
