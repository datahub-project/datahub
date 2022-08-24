import React, { useState } from 'react';
import styled from 'styled-components';

import { FacetFilterInput, FacetMetadata, SearchCondition } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { capitalizeFirstLetter } from '../shared/textUtil';
import { SearchFilterLabel } from './SearchFilterLabel';
import { SelectFilterValueModal } from './SelectFilterValueModal';

type Props = {
    facet: FacetMetadata;
    filter: FacetFilterInput;
    onClose: () => void;
    onUpdate: (newValue: FacetFilterInput) => void;
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

const StyledSearchFilterLabel = styled.div`
    margin: 4px;
`;

const FilterFieldLabel = styled.span`
    font-weight: 600;
`;

const conditionToReadable = {
    [SearchCondition.Contain]: 'contain',
    [SearchCondition.Equal]: 'is',
    [SearchCondition.In]: 'in',
};

export const AdvancedSearchFilter = ({ facet, filter, onClose, onUpdate }: Props) => {
    const [isEditing, setIsEditing] = useState(false);
    return (
        <FilterContainer>
            <FieldFilterSection>
                <span>
                    <FilterFieldLabel>{capitalizeFirstLetter(filter.field)} </FilterFieldLabel>
                    {conditionToReadable[filter.condition || SearchCondition.Contain]}
                </span>
                <CloseSpan role="button" onClick={onClose} tabIndex={0} onKeyPress={onClose}>
                    x
                </CloseSpan>
            </FieldFilterSection>
            <ValueFilterSection
                onClick={() => {
                    setIsEditing(!isEditing);
                }}
            >
                {filter.values.map((value) => (
                    <StyledSearchFilterLabel>
                        <SearchFilterLabel
                            hideCount
                            aggregation={
                                facet.aggregations.find((aggregation) => aggregation.value === value) ||
                                facet.aggregations[0]
                            }
                            field={value}
                        />
                    </StyledSearchFilterLabel>
                ))}
            </ValueFilterSection>
            {isEditing && (
                <SelectFilterValueModal
                    onCloseModal={() => setIsEditing(false)}
                    filterField={filter.field}
                    onSelect={(values) => {
                        const newFilter: FacetFilterInput = {
                            field: filter.field,
                            values: values as string[],
                            condition: filter.condition,
                        };
                        onUpdate(newFilter);
                    }}
                    initialUrns={filter.values}
                />
            )}
        </FilterContainer>
    );
};
