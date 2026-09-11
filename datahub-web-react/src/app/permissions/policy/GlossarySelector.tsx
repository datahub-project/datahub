import { Input, Text } from '@components';
import React, { useCallback, useMemo } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import ConditionSelectDropdown from '@app/permissions/policy/ConditionSelectDropdown';
import { useClearOnConditionChange } from '@app/permissions/policy/PolicyPrivilegeForm/useClearOnConditionChange';
import { FIELD_TYPES } from '@app/permissions/policy/constants';
import {
    createCriterionValueWithEntity,
    getFieldValues,
    setFieldValues,
    toStartsWithValues,
} from '@app/permissions/policy/policyUtils';
import GlossarySelect from '@app/sharedV2/glossary/GlossarySelect';

import { PolicyMatchCondition, PolicyMatchCriterionValue, ResourceFilter } from '@types';

const FieldWithConditionWrapper = styled.div`
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

const DescriptionText = styled(Text)`
    display: block;
    margin-bottom: 8px;
`;

const StyledInput = styled(Input)`
    width: 100%;
`;

type Props = {
    resources: ResourceFilter;
    setResources: (resources: ResourceFilter) => void;
    glossaryCondition: PolicyMatchCondition;
    setGlossaryCondition: (condition: PolicyMatchCondition) => void;
};

export default function GlossarySelector({ resources, setResources, glossaryCondition, setGlossaryCondition }: Props) {
    const { t } = useTranslation('settings.permissions');

    const glossaryEntities = useMemo(
        () => getFieldValues(resources.filter, FIELD_TYPES.GLOSSARY) || [],
        [resources.filter],
    );
    const glossarySelectValue = glossaryEntities.map((criterionValue) => criterionValue.value);

    // Seeds the cache so existing terms render real names instead of urn fallbacks.
    const glossaryDefaultValues = useMemo(
        () => glossaryEntities.map((criterionValue) => ({ urn: criterionValue.value, entity: criterionValue.entity })),
        [glossaryEntities],
    );

    const handleGlossaryUpdate = (urns: string[]) => {
        const filter = resources.filter || {
            criteria: [],
        };

        // Keep hydrated entities; rebuilding with null would drop resolved names on the first edit.
        const updatedGlossaryEntities: PolicyMatchCriterionValue[] = urns.map((urn) =>
            createCriterionValueWithEntity(
                urn,
                glossaryEntities.find((criterionValue) => criterionValue.value === urn)?.entity ?? null,
            ),
        );

        const updatedFilter = setFieldValues(filter, FIELD_TYPES.GLOSSARY, updatedGlossaryEntities, glossaryCondition);
        setResources({
            ...resources,
            filter: updatedFilter,
        });
    };

    const isStartsWithCondition = glossaryCondition === PolicyMatchCondition.StartsWith;
    const startsWithValue = isStartsWithCondition && glossarySelectValue.length > 0 ? glossarySelectValue[0] : '';

    const handleConditionChange = useClearOnConditionChange(
        glossaryCondition,
        FIELD_TYPES.GLOSSARY,
        useCallback(
            (newCondition: PolicyMatchCondition, updatedResources: ResourceFilter) => {
                setGlossaryCondition(newCondition);
                setResources(updatedResources);
            },
            [setGlossaryCondition, setResources],
        ),
    );

    return (
        <>
            <DescriptionText type="p" color="textSecondary">
                <Trans t={t} i18nKey="glossarySelectorDescription" components={{ bold: <b /> }} />
            </DescriptionText>
            <FieldWithConditionWrapper>
                <ConditionSelectDropdown
                    condition={glossaryCondition}
                    onConditionChange={handleConditionChange}
                    fieldType={FIELD_TYPES.GLOSSARY}
                    hasValues={glossaryEntities && glossaryEntities.length > 0}
                    resources={resources}
                />
                <SelectContainer>
                    {isStartsWithCondition ? (
                        <StyledInput
                            placeholder={t('privilegeForm.glossaryPrefixPlaceholder')}
                            value={startsWithValue}
                            onChange={(e) => handleGlossaryUpdate(toStartsWithValues(e.target.value))}
                        />
                    ) : (
                        <GlossarySelect
                            selectedUrns={glossarySelectValue}
                            defaultValues={glossaryDefaultValues}
                            onUpdate={handleGlossaryUpdate}
                            placeholder={t('glossarySelectorPlaceholder')}
                            width="full"
                            showSearch
                            areNodesSelectable
                        />
                    )}
                </SelectContainer>
            </FieldWithConditionWrapper>
        </>
    );
}
