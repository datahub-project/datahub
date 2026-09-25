import { MultiValueInput } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import DomainSelector from '@app/entityV2/shared/DomainSelector/DomainSelector';
import ConditionSelectDropdown from '@app/permissions/policy/ConditionSelectDropdown';
import { useClearOnConditionChange } from '@app/permissions/policy/PolicyPrivilegeForm/useClearOnConditionChange';
import { FIELD_TYPES } from '@app/permissions/policy/constants';

import { PolicyMatchCondition, ResourceFilter } from '@types';

type Props = {
    domainSelectValue: string[];
    domainCondition: PolicyMatchCondition;
    onConditionChange: (condition: PolicyMatchCondition, updatedResources: ResourceFilter) => void;
    onDomainsChange: (domainUrns: string[]) => void;
    resources: ResourceFilter;
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

export default function DomainsSelect({
    domainSelectValue,
    domainCondition,
    onConditionChange,
    onDomainsChange,
    resources,
}: Props) {
    const { t } = useTranslation('settings.permissions');

    const isStartsWithCondition = domainCondition === PolicyMatchCondition.StartsWith;

    const handleConditionChange = useClearOnConditionChange(domainCondition, FIELD_TYPES.DOMAIN, onConditionChange);

    return (
        <FieldWrapper>
            <ConditionSelectDropdown
                condition={domainCondition}
                onConditionChange={handleConditionChange}
                fieldType={FIELD_TYPES.DOMAIN}
                hasValues={domainSelectValue && domainSelectValue.length > 0}
                resources={resources}
            />
            <SelectContainer>
                {isStartsWithCondition ? (
                    <MultiValueInput
                        placeholder={t('privilegeForm.domainPrefixPlaceholder')}
                        values={domainSelectValue}
                        onUpdate={onDomainsChange}
                        width="full"
                    />
                ) : (
                    <DomainSelector
                        selectedDomains={domainSelectValue}
                        onDomainsChange={onDomainsChange}
                        placeholder={t('privilegeForm.domainPlaceholder')}
                        label=""
                        isMultiSelect
                        selectChildrenWithParent={false}
                    />
                )}
            </SelectContainer>
        </FieldWrapper>
    );
}
