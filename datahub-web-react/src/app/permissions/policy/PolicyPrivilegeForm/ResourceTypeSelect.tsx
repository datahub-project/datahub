import { Input, SimpleSelect } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import ConditionSelectDropdown from '@app/permissions/policy/ConditionSelectDropdown';
import { useClearOnConditionChange } from '@app/permissions/policy/PolicyPrivilegeForm/useClearOnConditionChange';
import { FIELD_TYPES } from '@app/permissions/policy/constants';

import { PolicyMatchCondition, ResourceFilter } from '@types';

type Props = {
    resourceTypeSelectValue: string[];
    resourceTypes: any[];
    resourceTypeCondition: PolicyMatchCondition;
    onConditionChange: (condition: PolicyMatchCondition, updatedResources: ResourceFilter) => void;
    onResourceTypesChange: (resourceTypes: string[]) => void;
    resources: ResourceFilter;
    resourcePrivileges: any[];
};

const FieldWithConditionWrapper = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    width: 100%;
`;

const SelectContainer = styled.div`
    flex: 1;
    min-width: 0;
`;

const StyledInput = styled(Input)`
    width: 100%;
`;

export default function ResourceTypeSelect({
    resourceTypeSelectValue,
    resourceTypes,
    resourceTypeCondition,
    onConditionChange,
    onResourceTypesChange,
    resources,
    resourcePrivileges,
}: Props) {
    const { t } = useTranslation('settings.permissions');

    const isStartsWithCondition = resourceTypeCondition === PolicyMatchCondition.StartsWith;
    const startsWithValue =
        isStartsWithCondition && resourceTypeSelectValue.length > 0 ? resourceTypeSelectValue[0] : '';

    const handleConditionChange = useClearOnConditionChange(
        resourceTypeCondition,
        FIELD_TYPES.RESOURCE_TYPE,
        onConditionChange,
    );

    return (
        <FieldWithConditionWrapper>
            <ConditionSelectDropdown
                condition={resourceTypeCondition}
                onConditionChange={handleConditionChange}
                fieldType={FIELD_TYPES.RESOURCE_TYPE}
                hasValues={resourceTypes && resourceTypes.length > 0}
                resources={resources}
            />
            <SelectContainer>
                {isStartsWithCondition ? (
                    <StyledInput
                        placeholder={t('privilegeForm.resourceTypePatternPlaceholder')}
                        value={startsWithValue}
                        onChange={(e) => onResourceTypesChange([e.target.value])}
                    />
                ) : (
                    <SimpleSelect
                        width="full"
                        isMultiSelect
                        showSearch
                        dataTestId="resource-type"
                        values={resourceTypeSelectValue}
                        placeholder={t('privilegeForm.resourceTypePlaceholder')}
                        onUpdate={(values) => {
                            onResourceTypesChange(values);
                        }}
                        options={resourcePrivileges
                            .filter((privs) => privs.resourceType !== 'all')
                            .map((resPrivs) => ({
                                value: resPrivs.resourceType,
                                label: resPrivs.resourceTypeDisplayName,
                            }))}
                        renderCustomSelectedValue={() => null}
                    />
                )}
            </SelectContainer>
        </FieldWithConditionWrapper>
    );
}
