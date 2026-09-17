import { Button } from '@components';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import ValueInput from '@app/permissions/policy/PolicyPrivilegeForm/ValueInput';
import PropertySelectField from '@app/permissions/policy/structuredProperties/PropertySelectField';
import {
    StructuredPropertyDefinition,
    StructuredPropertyValue,
    getEntityTypes,
    getValueOptions,
    getValueType,
} from '@app/permissions/policy/structuredProperties/utils';

import { useGetStructuredPropertyQuery } from '@graphql/structuredProperties.generated';

type Props = {
    property: StructuredPropertyValue;
    onPropertyChange: (propertyUrn: string) => void;
    onValueChange: (values: string[]) => void;
    onDelete: () => void;
};

const PropertyRow = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    width: 100%;
`;

const PropertySelectContainer = styled.div`
    flex: 1;
    min-width: 0;
`;

const ValueInputContainer = styled.div`
    flex: 1;
    min-width: 0;
`;

const DeleteButton = styled(Button)`
    flex-shrink: 0;
`;

export default function StructuredPropertyRow({ property, onPropertyChange, onValueChange, onDelete }: Props) {
    // Fetch selected property details for value input
    const { data: selectedPropertyData } = useGetStructuredPropertyQuery({
        variables: { urn: property.propertyUrn || '' },
        skip: !property.propertyUrn,
        fetchPolicy: 'cache-first',
    });

    const getPropertyDefinition = useMemo(() => {
        if (!selectedPropertyData?.entity?.urn) return null;
        return selectedPropertyData.entity as StructuredPropertyDefinition;
    }, [selectedPropertyData]);

    const allowedEntityTypeMemo = useMemo(
        () => (getPropertyDefinition ? getEntityTypes(getPropertyDefinition) : []),
        [getPropertyDefinition],
    );

    const allowedValuesMemo = useMemo(
        () => (getPropertyDefinition ? getValueOptions(getPropertyDefinition) : []),
        [getPropertyDefinition],
    );

    const valueTypeUrnMemo = useMemo(
        () => (getPropertyDefinition ? getValueType(getPropertyDefinition) : undefined),
        [getPropertyDefinition],
    );

    const propertyKey = property.propertyUrn ? `${property.propertyUrn}` : 'empty';

    return (
        <PropertyRow key={propertyKey} data-testid={`property-row-${propertyKey}`}>
            <PropertySelectContainer>
                <PropertySelectField selectedPropertyUrn={property.propertyUrn} onPropertyChange={onPropertyChange} />
            </PropertySelectContainer>
            {property.propertyUrn && getPropertyDefinition && (
                <ValueInputContainer>
                    <ValueInput
                        values={property.values}
                        valueTypeUrn={valueTypeUrnMemo}
                        allowedValues={allowedValuesMemo}
                        allowedEntityTypes={allowedEntityTypeMemo}
                        onUpdate={(newValues) => onValueChange(newValues)}
                    />
                </ValueInputContainer>
            )}
            <DeleteButton variant="text" color="gray" onClick={onDelete}>
                <Trash />
            </DeleteButton>
        </PropertyRow>
    );
}
