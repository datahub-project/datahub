import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
// import styled from 'styled-components';

import { FacetFilterInput } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { FIELDS_WHO_USE_CONTAINS_OPERATOR } from './utils/constants';

type Props = {
    filter: FacetFilterInput;
    onUpdate: (newValue: FacetFilterInput) => void;
};

const { Option } = Select;

// We track which fields are collection fields for the purpose of printing the conditions
// in a more gramatically correct way. On the backend they are handled the same.
const filtersOnNonCollectionFields = [
    'description',
    'fieldDescriptions',
    'fieldPaths',
    'removed',
    'typeNames',
    'entity',
    'domains',
    'origin',
];

function getLabelsForField(field: string) {
    if (FIELDS_WHO_USE_CONTAINS_OPERATOR.indexOf(field) >= 0) {
        return {
            default: 'contains',
            negated: 'does not contain',
        };
    }
    if (filtersOnNonCollectionFields.indexOf(field) >= 0) {
        return {
            default: 'equals',
            negated: 'not equal',
        };
    }

    // collection field
    return {
        default: 'is either of',
        negated: 'is not',
    };
}

const StyledSelect = styled(Select)`
    border-radius: 5px;
    background: ${ANTD_GRAY[4]};
    :hover {
        background: ${ANTD_GRAY[4.5]};
    }
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
                disabled={filter.field === 'entity'}
                dropdownMatchSelectWidth={false}
            >
                {Object.keys(labelsForField).map((labelKey) => (
                    <Option value={labelKey}>{labelsForField[labelKey]}</Option>
                ))}
            </StyledSelect>
        </>
    );
};
