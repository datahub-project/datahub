import * as React from 'react';
import styled from 'styled-components';

import { FacetFilterInput, FacetMetadata, SearchCondition } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { capitalizeFirstLetter } from '../shared/textUtil';
import { SearchFilterLabel } from './SearchFilterLabel';

type Props = {
    facet: FacetMetadata;
    filter: FacetFilterInput;
    onClose: () => void;
};

const FilterContainer = styled.div`
    border-radius: 12px;
    border: 1px solid ${ANTD_GRAY[5]};
    padding: 4px;
    margin: 4px;
`;

const FieldFilterSection = styled.span`
    color: ${ANTD_GRAY[9]};
    padding: 4px;
    display: flex;
    justify-content: space-between;
`;

const ValueFilterSection = styled.div`
    padding: 4px;
    border-top: 1px solid ${ANTD_GRAY[5]};
`;

const CloseSpan = styled.span`
    :hover {
        cursor: pointer;
    }
`;

const conditionToReadable = {
    [SearchCondition.Contain]: 'contains',
    [SearchCondition.Equal]: 'is',
    [SearchCondition.In]: 'in',
};

export const AdvancedSearchFilter = ({ facet, filter, onClose }: Props) => {
    return (
        <FilterContainer>
            <FieldFilterSection>
                <span>
                    {capitalizeFirstLetter(filter.field)}{' '}
                    {conditionToReadable[filter.condition || SearchCondition.Contain]}
                </span>
                <CloseSpan role="button" onClick={onClose} tabIndex={0} onKeyPress={onClose}>
                    x
                </CloseSpan>
            </FieldFilterSection>
            <ValueFilterSection>
                {filter.values.map((value) => (
                    <SearchFilterLabel
                        hideCounts
                        aggregation={
                            facet.aggregations.find((aggregation) => aggregation.value === value) ||
                            facet.aggregations[0]
                        }
                        field={value}
                    />
                ))}
            </ValueFilterSection>
        </FilterContainer>
    );
};
