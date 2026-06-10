import { Select } from 'antd';
import { TFunction } from 'i18next';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import {
    DESCRIPTION_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    FIELDS_THAT_USE_CONTAINS_OPERATOR,
    FIELD_DESCRIPTIONS_FILTER_NAME,
    FIELD_PATHS_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    REMOVED_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
} from '@app/search/utils/constants';

import { FacetFilterInput } from '@types';

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

function getLabelsForField(field: string, t: TFunction<'search'>) {
    if (FIELDS_THAT_USE_CONTAINS_OPERATOR.includes(field)) {
        return {
            default: t('condition.contains'),
            negated: t('condition.doesNotContain'),
        };
    }
    if (filtersOnNonCollectionFields.includes(field)) {
        return {
            default: t('condition.equals'),
            negated: t('condition.notEqual'),
        };
    }

    // collection field
    return {
        default: t('condition.isAnyOf'),
        negated: t('condition.isNot'),
    };
}

const StyledSelect = styled(Select)`
    border-radius: 5px;
    color: ${(props) => props.theme.colors.text};
    background: ${(props) => props.theme.colors.bgSurface};
    :hover {
        background: ${(props) => props.theme.colors.border};
    }
    width: auto;
`;

export const AdvancedSearchFilterConditionSelect = ({ filter, onUpdate }: Props) => {
    const { t } = useTranslation('search');
    const labelsForField = getLabelsForField(filter.field, t);

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
                dropdownMatchSelectWidth={false}
            >
                {Object.keys(labelsForField).map((labelKey) => (
                    <Option key={labelKey} value={labelKey}>
                        {labelsForField[labelKey]}
                    </Option>
                ))}
            </StyledSelect>
        </>
    );
};
