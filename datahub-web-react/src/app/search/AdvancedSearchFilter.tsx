import { CloseOutlined } from '@ant-design/icons';
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
    border-radius: 5px;
    border: 1px solid ${ANTD_GRAY[5]};
    padding: 4px;
    margin: 4px;
    :hover {
        cursor: pointer;
        background: ${ANTD_GRAY[2]};
    }
`;

const FieldFilterSection = styled.span`
    color: ${ANTD_GRAY[9]};
    padding: 4px;
    display: flex;
    justify-content: space-between;
`;

const ValueFilterSection = styled.div`
    :hover {
        cursor: pointer;
    }
`;

const CloseSpan = styled.span`
    :hover {
        color: black;
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
        <>
            <FilterContainer
                onClick={() => {
                    setIsEditing(!isEditing);
                }}
            >
                <FieldFilterSection>
                    <span>
                        <FilterFieldLabel>{FIELD_TO_LABEL[filter.field]} </FilterFieldLabel>
                        <AdvancedSearchFilterConditionSelect filter={filter} onUpdate={onUpdate} />
                    </span>
                    <CloseSpan
                        role="button"
                        onClick={(e) => {
                            e.preventDefault();
                            e.stopPropagation();
                            onClose();
                        }}
                        tabIndex={0}
                        onKeyPress={onClose}
                    >
                        <CloseOutlined />
                    </CloseSpan>
                </FieldFilterSection>
                <ValueFilterSection>
                    {TEXT_FILTERS.indexOf(filter.field) === -1 &&
                        filter.values.map((value) => {
                            const matchedAggregation = facet?.aggregations?.find(
                                (aggregation) => aggregation.value === value,
                            );
                            if (!matchedAggregation) return null;

                            return (
                                <StyledSearchFilterLabel>
                                    <SearchFilterLabel hideCount aggregation={matchedAggregation} field={value} />
                                </StyledSearchFilterLabel>
                            );
                        })}
                    {TEXT_FILTERS.indexOf(filter.field) !== -1 && filter.values.map((value) => <span>{value}</span>)}
                </ValueFilterSection>
            </FilterContainer>
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
                            negated: filter.negated || false,
                        };
                        onUpdate(newFilter);
                    }}
                    initialValues={filter.values}
                />
            )}
        </>
    );
};
