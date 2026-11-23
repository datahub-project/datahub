import { MultiValueInput, SimpleSelect } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import ConditionSelectDropdown from '@app/permissions/policy/ConditionSelectDropdown';
import { getEnvironmentOptions } from '@app/permissions/policy/PolicyPrivilegeForm/EnvironmentsSelect.utils';
import { useClearOnConditionChange } from '@app/permissions/policy/PolicyPrivilegeForm/useClearOnConditionChange';
import { FIELD_TYPES } from '@app/permissions/policy/constants';

import { useListCustomFabricTypesQuery } from '@graphql/customFabricType.generated';
import { CustomFabricType, PolicyMatchCondition, ResourceFilter } from '@types';

// Environments are few; one call lists them all.
const MAX_ENVIRONMENTS = 1000;

type Props = {
    environmentSelectValue: string[];
    environmentCondition: PolicyMatchCondition;
    onConditionChange: (condition: PolicyMatchCondition, updatedResources: ResourceFilter) => void;
    onEnvironmentsChange: (environments: string[]) => void;
    resources: ResourceFilter;
    // Custom environments are listed only when they are enabled on the server.
    customEnvironmentsEnabled: boolean;
};

const FieldWrapper = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    width: 100%;
    min-width: 0;
`;

const SelectContainer = styled.div`
    flex: 1;
    min-width: 0;
`;

export default function EnvironmentsSelect({
    environmentSelectValue,
    environmentCondition,
    onConditionChange,
    onEnvironmentsChange,
    resources,
    customEnvironmentsEnabled,
}: Props) {
    const { t } = useTranslation('settings.permissions');
    const { data } = useListCustomFabricTypesQuery({
        variables: { input: { start: 0, count: MAX_ENVIRONMENTS, includeDeprecated: true } },
        skip: !customEnvironmentsEnabled,
    });
    const customEnvironments = (data?.listCustomFabricTypes?.customFabricTypes || []) as CustomFabricType[];

    const handleConditionChange = useClearOnConditionChange(environmentCondition, FIELD_TYPES.ENV, onConditionChange);

    return (
        <FieldWrapper>
            <ConditionSelectDropdown
                condition={environmentCondition}
                onConditionChange={handleConditionChange}
                fieldType={FIELD_TYPES.ENV}
                hasValues={environmentSelectValue.length > 0}
                resources={resources}
            />
            <SelectContainer>
                {environmentCondition === PolicyMatchCondition.StartsWith ? (
                    <MultiValueInput
                        placeholder={t('privilegeForm.environmentPrefixPlaceholder')}
                        values={environmentSelectValue}
                        onUpdate={onEnvironmentsChange}
                        width="full"
                    />
                ) : (
                    <SimpleSelect
                        width="full"
                        isMultiSelect
                        showSearch
                        dataTestId="environment-select"
                        values={environmentSelectValue}
                        placeholder={t('privilegeForm.environmentPlaceholder')}
                        onUpdate={onEnvironmentsChange}
                        options={getEnvironmentOptions(customEnvironments, environmentSelectValue)}
                    />
                )}
            </SelectContainer>
        </FieldWrapper>
    );
}
