import { Input } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { EntitySearchSelect } from '@app/entityV2/shared/EntitySearchSelect/EntitySearchSelect';
import ConditionSelectDropdown from '@app/permissions/policy/ConditionSelectDropdown';
import { useClearOnConditionChange } from '@app/permissions/policy/PolicyPrivilegeForm/useClearOnConditionChange';
import { FIELD_TYPES } from '@app/permissions/policy/constants';
import { mapResourceTypeToEntityType } from '@app/permissions/policy/policyUtils';

import { EntityType, PolicyMatchCondition, ResourceFilter } from '@types';

type Props = {
    resourceSelectValue: string[];
    resourceEntities: any[];
    resourceCondition: PolicyMatchCondition;
    onConditionChange: (condition: PolicyMatchCondition, updatedResources: ResourceFilter) => void;
    onResourcesChange: (resources: string[]) => void;
    resources: ResourceFilter;
    resourceTypeSelectValue: string[];
    resourcePrivileges: any[];
    resourceTypeCondition: PolicyMatchCondition;
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

export default function ResourceSelect({
    resourceSelectValue,
    resourceEntities,
    resourceCondition,
    onConditionChange,
    onResourcesChange,
    resources,
    resourceTypeSelectValue,
    resourcePrivileges,
    resourceTypeCondition,
}: Props) {
    const { t } = useTranslation('settings.permissions');

    const isStartsWithCondition = resourceCondition === PolicyMatchCondition.StartsWith;
    const startswithValue = isStartsWithCondition && resourceSelectValue.length > 0 ? resourceSelectValue[0] : '';

    const handleConditionChange = useClearOnConditionChange(resourceCondition, FIELD_TYPES.RESOURCE, onConditionChange);

    // Calculate entity types based on resource type condition
    const entityTypesForSearch = useMemo(() => {
        // Equals condition: search only selected resource types' entity types
        if (resourceTypeCondition === PolicyMatchCondition.Equals) {
            if (!resourceTypeSelectValue?.length) {
                return [];
            }
            return resourceTypeSelectValue
                .map((resourceType) => mapResourceTypeToEntityType(resourceType, resourcePrivileges))
                .filter((entityType): entityType is EntityType => !!entityType);
        }

        // For NotEquals and StartsWith, search all types
        return [];
    }, [resourceTypeCondition, resourceTypeSelectValue, resourcePrivileges]);

    return (
        <FieldWithConditionWrapper>
            <ConditionSelectDropdown
                condition={resourceCondition}
                onConditionChange={handleConditionChange}
                fieldType={FIELD_TYPES.RESOURCE}
                hasValues={resourceEntities && resourceEntities.length > 0}
                resources={resources}
            />
            <SelectContainer>
                {isStartsWithCondition ? (
                    <StyledInput
                        placeholder={t('privilegeForm.resourcePrefixPlaceholder')}
                        value={startswithValue}
                        onChange={(e) => onResourcesChange([e.target.value])}
                    />
                ) : (
                    <EntitySearchSelect
                        selectedUrns={resourceSelectValue}
                        entityTypes={entityTypesForSearch}
                        isMultiSelect
                        onUpdate={onResourcesChange}
                        placeholder={t('privilegeForm.resourcePlaceholder')}
                        width="full"
                        showClear
                    />
                )}
            </SelectContainer>
        </FieldWithConditionWrapper>
    );
}
