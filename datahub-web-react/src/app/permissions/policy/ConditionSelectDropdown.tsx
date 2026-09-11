import { SimpleSelect } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { FIELD_TYPES } from '@app/permissions/policy/constants';
import { setFieldCondition } from '@app/permissions/policy/policyUtils';

import { PolicyMatchCondition, ResourceFilter } from '@types';

const StyledSimpleSelect = styled(SimpleSelect)`
    min-width: 160px;
`;

type ConditionSelectDropdownProps = {
    condition: PolicyMatchCondition;
    onConditionChange: (condition: PolicyMatchCondition, updatedResources: ResourceFilter) => void;
    fieldType: (typeof FIELD_TYPES)[keyof typeof FIELD_TYPES];
    hasValues: boolean;
    resources: ResourceFilter;
};

export default function ConditionSelectDropdown({
    condition,
    onConditionChange,
    fieldType,
    hasValues,
    resources,
}: ConditionSelectDropdownProps) {
    const { t } = useTranslation('settings.permissions');

    // Privileges derive from the selected types, so a prefix leaves none selectable.
    const supportsStartsWith = fieldType !== FIELD_TYPES.RESOURCE_TYPE || condition === PolicyMatchCondition.StartsWith;

    const options = [
        { value: PolicyMatchCondition.Equals, label: t('policyForm.condition.equals') },
        { value: PolicyMatchCondition.NotEquals, label: t('policyForm.condition.notEquals') },
        ...(supportsStartsWith
            ? [{ value: PolicyMatchCondition.StartsWith, label: t('policyForm.condition.startsWith') }]
            : []),
    ];

    return (
        <StyledSimpleSelect
            dataTestId={`condition-${fieldType}`}
            options={options}
            values={[condition]}
            onUpdate={(values) => {
                const newCondition = values[0] as PolicyMatchCondition;
                if (hasValues) {
                    const updatedResources = {
                        ...resources,
                        filter: setFieldCondition(resources.filter || { criteria: [] }, fieldType, newCondition),
                    };
                    onConditionChange(newCondition, updatedResources);
                } else {
                    onConditionChange(newCondition, resources);
                }
            }}
            isMultiSelect={false}
            showClear={false}
            width="fit-content"
        />
    );
}
