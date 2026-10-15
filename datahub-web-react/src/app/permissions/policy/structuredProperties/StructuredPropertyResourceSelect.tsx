import { Button } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import ConditionSelectDropdown from '@app/permissions/policy/ConditionSelectDropdown';
import { useClearOnConditionChange } from '@app/permissions/policy/PolicyPrivilegeForm/useClearOnConditionChange';
import { FIELD_TYPES } from '@app/permissions/policy/constants';
import StructuredPropertyRow from '@app/permissions/policy/structuredProperties/StructuredPropertyRow';
import { StructuredPropertyValue } from '@app/permissions/policy/structuredProperties/utils';

import { PolicyMatchCondition, ResourceFilter } from '@types';

type Props = {
    structuredProperties: StructuredPropertyValue[];
    condition: PolicyMatchCondition;
    onConditionChange: (condition: PolicyMatchCondition, updatedResources: ResourceFilter) => void;
    onStructuredPropertiesChange: (properties: StructuredPropertyValue[]) => void;
    resources: ResourceFilter;
};

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    width: 100%;
`;

const MainRow = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    width: 100%;
`;

const ConditionSelect = styled.div`
    flex-shrink: 0;
`;

const PropertyRowsContainer = styled.div<{ $hasRows: boolean }>`
    display: flex;
    flex-direction: column;
    gap: 8px;
    ${(props) => props.$hasRows && 'flex: 1; min-width: 0;'}
`;

const AddPropertyButton = styled(Button)`
    width: fit-content;
    flex-shrink: 0;
`;

export default function StructuredPropertyResourceSelect({
    structuredProperties,
    condition,
    onConditionChange,
    onStructuredPropertiesChange,
    resources,
}: Props) {
    const { t } = useTranslation('settings.permissions');
    const initializedRef = useRef(false);

    useEffect(() => {
        if (!initializedRef.current && structuredProperties.length === 0) {
            initializedRef.current = true;
            onStructuredPropertiesChange([{ propertyUrn: '', values: [] }]);
        }
    }, [structuredProperties.length, onStructuredPropertiesChange]);

    const handlePropertyChange = (index: number, propertyUrn: string) => {
        const updated = structuredProperties.map((prop, i) =>
            i === index ? { ...prop, propertyUrn, values: [] } : prop,
        );
        onStructuredPropertiesChange(updated);
    };

    const handleValueChange = (index: number, values: string[]) => {
        const updated = structuredProperties.map((prop, i) => (i === index ? { ...prop, values } : prop));
        onStructuredPropertiesChange(updated);
    };

    const handleDeleteRow = (index: number) => {
        const updated = structuredProperties.filter((_, i) => i !== index);
        onStructuredPropertiesChange(updated);
    };

    const handleAddProperty = () => {
        const updated = [...structuredProperties, { propertyUrn: '', values: [] }];
        onStructuredPropertiesChange(updated);
    };

    const handleConditionChange = useClearOnConditionChange(
        condition,
        FIELD_TYPES.STRUCTURED_PROPERTY,
        onConditionChange,
    );

    const hasValues = structuredProperties.some((p) => p.propertyUrn && p.values.length > 0);

    return (
        <Container>
            <MainRow>
                <ConditionSelect>
                    <ConditionSelectDropdown
                        condition={condition}
                        onConditionChange={handleConditionChange}
                        fieldType={FIELD_TYPES.STRUCTURED_PROPERTY}
                        hasValues={hasValues}
                        resources={resources}
                    />
                </ConditionSelect>

                <PropertyRowsContainer $hasRows={structuredProperties.length > 0}>
                    {structuredProperties.map((property, index) => {
                        const propertyKey = property.propertyUrn
                            ? `${property.propertyUrn}-${index}`
                            : `empty-${index}`;

                        return (
                            <StructuredPropertyRow
                                key={propertyKey}
                                property={property}
                                onPropertyChange={(propertyUrn) => handlePropertyChange(index, propertyUrn)}
                                onValueChange={(values) => handleValueChange(index, values)}
                                onDelete={() => handleDeleteRow(index)}
                            />
                        );
                    })}
                </PropertyRowsContainer>

                <AddPropertyButton variant="text" icon={{ icon: Plus }} onClick={handleAddProperty}>
                    {t('privilegeForm.addProperty')}
                </AddPropertyButton>
            </MainRow>
        </Container>
    );
}
