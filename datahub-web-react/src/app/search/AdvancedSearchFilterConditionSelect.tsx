import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { FacetFilterInput } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import {
    DESCRIPTION_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    ENTITY_FILTER_NAME,
    FIELDS_THAT_USE_CONTAINS_OPERATOR,
    FIELD_DESCRIPTIONS_FILTER_NAME,
    FIELD_PATHS_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    REMOVED_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
} from './utils/constants';

type Props = {
    filter: FacetFilterInput;
    onUpdate: (newValue: FacetFilterInput) => void;
};

const { Option } = Select;

// We track which fields are collection fields for the purpose of printing the conditions
// in a more gramatically correct way. On the backend they are handled the same.
const filtersOnNonCollectionFields = [
    DESCRIPTION_FILTER_NAME,
    FIELD_DESCRIPTIONS_FILTER_NAME,
    FIELD_PATHS_FILTER_NAME,
    REMOVED_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    ORIGIN_FILTER_NAME,
];

function getLabelsForField(field: string) {
    if (FIELDS_THAT_USE_CONTAINS_OPERATOR.includes(field)) {
        return {
            default: 'contains',
            negated: 'does not contain',
        };
    }
    if (filtersOnNonCollectionFields.includes(field)) {
        return {
            default: 'equals',
            negated: 'not equal',
        };
    }

    // collection field
    return {
        default: 'is any of',
        negated: 'is not',
    };
}

const StyledSelect = styled(Select)`
    border-radius: 5px;
    color: ${ANTD_GRAY[9]};
    background: ${ANTD_GRAY[3]};
    :hover {
        background: ${ANTD_GRAY[4.5]};
    }
    width: auto;
`;

export const AdvancedSearchFilterConditionSelect = ({ filter, onUpdate }: Props) => {
    const labelsForField = getLabelsForField(filter.field);

    const selectedValue = filter.negated ? 'negated' : 'default';

    return (
        <>
            <StyledSelect
                style={{}}
                // prevent the edit filter value modal from opening
                onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                }}
                showArrow={false}
                bordered={false}
                value={selectedValue}
                onChange={(newValue) => {
                    if (newValue !== selectedValue) {
                        onUpdate({
                            ...filter,
                            negated: newValue === 'negated',
                        });
                    }
                }}
                size="small"
                disabled={filter.field === ENTITY_FILTER_NAME}
                dropdownMatchSelectWidth={false}
            >
                {Object.keys(labelsForField).map((labelKey) => (
                    <Option value={labelKey}>{labelsForField[labelKey]}</Option>
                ))}
            </StyledSelect>
        </>
    );
};
