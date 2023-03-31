import { CloseOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';

import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { AdvancedSearchFilterConditionSelect } from './AdvancedSearchFilterConditionSelect';
import { AdvancedFilterSelectValueModal } from './AdvancedFilterSelectValueModal';
import { FIELD_TO_LABEL } from './utils/constants';
import { AdvancedSearchFilterValuesSection } from './AdvancedSearchFilterValuesSection';

type Props = {
    facet: FacetMetadata;
    filter: FacetFilterInput;
    onClose: () => void;
    onUpdate: (newValue: FacetFilterInput) => void;
    loading: boolean;
    isCompact?: boolean;
    disabled?: boolean;
};

const FilterContainer = styled.div<{ isCompact: boolean }>`
    box-shadow: 0px 0px 4px 0px #00000010;
    border-radius: 10px;
    border: 1px solid ${ANTD_GRAY[4]};
    padding: ${(props) => (props.isCompact ? '0 4px' : '4px')};
    margin: ${(props) => (props.isCompact ? '0 4px 4px 4px' : '4px')};
    :hover {
        cursor: pointer;
        background: ${ANTD_GRAY[2]};
    }
`;

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

const CloseSpan = styled.span`
    :hover {
        color: black;
    }
`;

const FilterFieldLabel = styled.span`
    font-weight: 600;
    margin-right: 2px;
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
                    {!loading && isCompact && <AdvancedSearchFilterValuesSection filter={filter} facet={facet} isCompact />}
                    {!disabled && (
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
                    )}
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
