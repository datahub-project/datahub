import * as React from 'react';
import styled from 'styled-components';

import { FacetFilterInput, SearchCondition } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { capitalizeFirstLetter } from '../shared/textUtil';
import { SearchFilterLabel } from './SearchFilterLabel';

type Props = {
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

const conditionToReadable = {
    [SearchCondition.Contain]: 'contains',
    [SearchCondition.Equal]: 'is',
    [SearchCondition.In]: 'in',
};

export const AdvancedSearchFilter = ({ filter, onClose }: Props) => {
    return (
        <FilterContainer>
            <FieldFilterSection>
                <span>
                    {capitalizeFirstLetter(filter.field)}{' '}
                    {conditionToReadable[filter.condition || SearchCondition.Contain]}
                </span>
                <button type="button" onClick={onClose}>
                    x
                </button>
            </FieldFilterSection>
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
