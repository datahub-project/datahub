import React, { useState } from 'react';
import styled from 'styled-components';

import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { AdvancedSearchFilterConditionSelect } from './AdvancedSearchFilterConditionSelect';
import { AdvancedFilterSelectValueModal } from './AdvancedFilterSelectValueModal';
import { ENTITY_SUB_TYPE_FILTER_NAME, FIELD_TO_LABEL } from './utils/constants';
import { AdvancedSearchFilterValuesSection } from './AdvancedSearchFilterValuesSection';
import AdvancedFilterCloseButton from './advanced/AdvancedFilterCloseButton';
import { FilterContainer } from './advanced/styles';
import EntitySubTypeAdvancedFilterLabel from './advanced/EntitySubTypeAdvancedFilterLabel';

type Props = {
    facet: FacetMetadata;
    filter: FacetFilterInput;
    onClose: () => void;
    onUpdate: (newValue: FacetFilterInput) => void;
    loading: boolean;
    isCompact?: boolean;
    disabled?: boolean;
};

const FieldFilterSection = styled.span<{ isCompact: boolean }>`
    color: ${ANTD_GRAY[9]};
    padding: ${(props) => (props.isCompact ? '2px 4px' : '4px')};
    display: flex;
    justify-content: space-between;

    ${(props) =>
        props.isCompact &&
        `
        display: flex;
        align-items: center;
    `}
`;

const FieldFilterSelect = styled.span<{ isCompact: boolean }>`
    padding-right: ${(props) => (props.isCompact ? '0' : '8px;')};
`;

const FilterFieldLabel = styled.span`
    font-weight: 600;
    margin-right: 4px;
`;

export const AdvancedSearchFilter = ({
    facet,
    filter,
    onClose,
    onUpdate,
    loading,
    isCompact = false,
    disabled = false,
}: Props) => {
    const [isEditing, setIsEditing] = useState(false);

    if (filter.field === ENTITY_SUB_TYPE_FILTER_NAME) {
        return (
            <EntitySubTypeAdvancedFilterLabel
                filter={filter}
                disabled={disabled}
                isCompact={isCompact}
                onClose={onClose}
            />
        );
    }

    return (
        <>
            <FilterContainer
                onClick={() => {
                    setIsEditing(!isEditing);
                }}
                isCompact={isCompact}
            >
                <FieldFilterSection isCompact={isCompact}>
                    <FieldFilterSelect isCompact={isCompact}>
                        <FilterFieldLabel>{FIELD_TO_LABEL[filter.field]} </FilterFieldLabel>
                        <AdvancedSearchFilterConditionSelect filter={filter} onUpdate={onUpdate} />
                    </FieldFilterSelect>
                    {!loading && isCompact && (
                        <AdvancedSearchFilterValuesSection filter={filter} facet={facet} isCompact />
                    )}
                    {!disabled && <AdvancedFilterCloseButton onClose={onClose} />}
                </FieldFilterSection>
                {!loading && !isCompact && <AdvancedSearchFilterValuesSection filter={filter} facet={facet} />}
            </FilterContainer>
            {!disabled && isEditing && (
                <AdvancedFilterSelectValueModal
                    facet={facet}
                    onCloseModal={() => setIsEditing(false)}
                    filterField={filter.field}
                    onSelect={(values) => {
                        const newFilter: FacetFilterInput = {
                            field: filter.field,
                            values: values as string[],
                            condition: filter.condition,
                            negated: filter.negated,
                        };
                        onUpdate(newFilter);
                    }}
                    initialValues={filter.values || []}
                />
            )}
        </>
    );
};
