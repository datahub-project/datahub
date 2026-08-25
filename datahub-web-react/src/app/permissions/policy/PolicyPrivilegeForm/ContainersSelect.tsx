import { Input } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { EntitySearchSelect } from '@app/entityV2/shared/EntitySearchSelect/EntitySearchSelect';
import ConditionSelectDropdown from '@app/permissions/policy/ConditionSelectDropdown';
import { useClearOnConditionChange } from '@app/permissions/policy/PolicyPrivilegeForm/useClearOnConditionChange';
import { FIELD_TYPES } from '@app/permissions/policy/constants';

import { EntityType, PolicyMatchCondition, ResourceFilter } from '@types';

type Props = {
    containerSelectValue: string[];
    containers: any[];
    containerCondition: PolicyMatchCondition;
    onConditionChange: (condition: PolicyMatchCondition, updatedResources: ResourceFilter) => void;
    onContainersChange: (containerUrns: string[]) => void;
    resources: ResourceFilter;
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

export default function ContainersSelect({
    containerSelectValue,
    containers,
    containerCondition,
    onConditionChange,
    onContainersChange,
    resources,
}: Props) {
    const { t } = useTranslation('settings.permissions');

    const isStartsWithCondition = containerCondition === PolicyMatchCondition.StartsWith;
    const startsWithValue = isStartsWithCondition && containerSelectValue.length > 0 ? containerSelectValue[0] : '';

    const handleConditionChange = useClearOnConditionChange(
        containerCondition,
        FIELD_TYPES.CONTAINER,
        onConditionChange,
    );

    // Memoize entityTypes to prevent unnecessary re-searches when parent re-renders
    const containerEntityTypes = useMemo(() => [EntityType.Container], []);

    return (
        <FieldWithConditionWrapper>
            <ConditionSelectDropdown
                condition={containerCondition}
                onConditionChange={handleConditionChange}
                fieldType={FIELD_TYPES.CONTAINER}
                hasValues={containers && containers.length > 0}
                resources={resources}
            />
            <SelectContainer>
                {isStartsWithCondition ? (
                    <StyledInput
                        placeholder={t('privilegeForm.containerPrefixPlaceholder')}
                        value={startsWithValue}
                        onChange={(e) => onContainersChange([e.target.value])}
                    />
                ) : (
                    <EntitySearchSelect
                        selectedUrns={containerSelectValue}
                        entityTypes={containerEntityTypes}
                        isMultiSelect
                        onUpdate={onContainersChange}
                        placeholder={t('privilegeForm.containerPlaceholder')}
                        width="full"
                        showClear
                    />
                )}
            </SelectContainer>
        </FieldWithConditionWrapper>
    );
}
