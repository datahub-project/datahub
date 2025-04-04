import React, { useMemo } from 'react';
import { EntityType, PropertyCardinality, StructuredPropertyEntity } from '../../../../../../../types.generated';
import { SearchSelectUrnInput } from './SearchSelectUrnInput';

interface StructuredPropertySearchSelectUrnInputProps {
    structuredProperty: StructuredPropertyEntity;
    selectedValues: string[];
    updateSelectedValues: (values: string[] | number[]) => void;
}

// Wrapper component that extracts information from StructuredProperty
export default function StructuredPropertySearchSelectUrnInput({
    structuredProperty,
    selectedValues,
    updateSelectedValues,
}: StructuredPropertySearchSelectUrnInputProps) {
    // Get the allowed entity types from the structured property
    const allowedEntityTypes = useMemo(() => {
        return (
            structuredProperty.definition.typeQualifier?.allowedTypes?.map(
                (allowedType) => allowedType.info.type as EntityType,
            ) || []
        );
    }, [structuredProperty]);

    const isMultiple = structuredProperty.definition.cardinality === PropertyCardinality.Multiple;

    return (
        <SearchSelectUrnInput
            allowedEntityTypes={allowedEntityTypes}
            isMultiple={isMultiple}
            selectedValues={selectedValues}
            updateSelectedValues={updateSelectedValues}
        />
    );
}
