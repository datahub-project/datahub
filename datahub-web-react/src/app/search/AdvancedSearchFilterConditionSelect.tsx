import { Select } from 'antd';
import React from 'react';
// import styled from 'styled-components';

import { FacetFilterInput } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';

type Props = {
    filter: FacetFilterInput;
    onUpdate: (newValue: FacetFilterInput) => void;
};

const { Option } = Select;

// We track which fields are collection fields for the purpose of printing the conditions
// in a more gramatically correct way. On the backend they are handled the same.
const filtersOnNonCollectionFields = ['description'];

// const LabelSelect = styled.span`
//     border-radius: 5px;
//     background: ${ANTD_GRAY[4]};
//     padding: 2px 10px;
//     font-size: 12px;
//     font-weight: 600;
//     line-height: 22px;
// `;

function getLabelsForField(field: string) {
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

export const AdvancedSearchFilterConditionSelect = ({ filter, onUpdate }: Props) => {
    const labelsForField = getLabelsForField(filter.field);

    const selectedValue = filter.negated ? 'negated' : 'default';

    return (
        <>
            <Select
                style={{
                    background: ANTD_GRAY[4],
                    borderRadius: 5,
                    width: 85,
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
            >
                {Object.keys(labelsForField).map((labelKey) => (
                    <Option value={labelKey}>{labelsForField[labelKey]}</Option>
                ))}
            </Select>
        </>
    );
};
