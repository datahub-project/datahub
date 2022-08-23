import * as React from 'react';
import styled from 'styled-components';

import { FacetFilterInput, SearchCondition } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { capitalizeFirstLetter } from '../shared/textUtil';
import { SearchFilterLabel } from './SearchFilterLabel';

type Props = {
    filter: FacetFilterInput;
};

const FilterContainer = styled.div`
    border-radius: 12px;
    border: 1px solid ${ANTD_GRAY[5]};
    padding: 4px;
    margin: 4px;
`;

const FieldFilterSection = styled.span`
    padding: 4px;
`;

const ValueFilterSection = styled.div`
    padding: 4px;
    border-top: 1px solid ${ANTD_GRAY[5]};
`;

const ConditionFilterSection = styled.span``;

const conditionToReadable = {
    [SearchCondition.Contain]: 'contains',
    [SearchCondition.Equal]: 'is',
    [SearchCondition.In]: 'in',
};

export const AdvancedSearchFilter = ({ filter }: Props) => {
    return (
        <FilterContainer>
            <FieldFilterSection>{capitalizeFirstLetter(filter.field)}</FieldFilterSection>
            <ConditionFilterSection>
                {conditionToReadable[filter.condition || SearchCondition.Contain]}
            </ConditionFilterSection>
            <ValueFilterSection>
                {filter.values.map((value) => (
                    <SearchFilterLabel
                        hideCounts
                        aggregation={{
                            value,
                            count: 1,
                            entity: null,
                        }}
                        field={value}
                    />
                ))}
            </ValueFilterSection>
        </FilterContainer>
    );
};
