import React, { useState } from 'react';
import styled from 'styled-components';

import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { AdvancedSearchFilterConditionSelect } from './AdvancedSearchFilterConditionSelect';
import { SearchFilterLabel } from './SearchFilterLabel';
import { SelectFilterValueModal } from './SelectFilterValueModal';
import { FIELD_TO_LABEL } from './utils/constants';

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

const TEXT_FILTERS = ['fieldPaths'];

export const AdvancedSearchFilter = ({ facet, filter, onClose, onUpdate }: Props) => {
    const [isEditing, setIsEditing] = useState(false);
    return (
        <FilterContainer>
            <FieldFilterSection>
                <span>
                    <FilterFieldLabel>{FIELD_TO_LABEL[filter.field]} </FilterFieldLabel>
                    <AdvancedSearchFilterConditionSelect filter={filter} onUpdate={onUpdate} />
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
                {TEXT_FILTERS.indexOf(filter.field) === -1 &&
                    filter.values.map((value) => (
                        <StyledSearchFilterLabel>
                            <SearchFilterLabel
                                hideCount
                                aggregation={
                                    facet?.aggregations?.find((aggregation) => aggregation.value === value) ||
                                    facet?.aggregations?.[0]
                                }
                                field={value}
                            />
                        </StyledSearchFilterLabel>
                    ))}
                {TEXT_FILTERS.indexOf(filter.field) !== -1 && filter.values.map((value) => <span>{value}</span>)}
            </ValueFilterSection>
            {isEditing && (
                <SelectFilterValueModal
                    facet={facet}
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
                    initialValues={filter.values}
                />
            )}
        </FilterContainer>
    );
};
